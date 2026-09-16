defmodule Troupe.CLI do
  @moduledoc """
  `mix troupe.*` 命令的实现内核。连接：`--node a@host`（需与目标节点共享 cookie；
  省略则为本地默认 troupe）。所有列表命令支持 `--json`。
  """

  @switches [
    node: :string,
    json: :boolean,
    name: :string,
    role: :string,
    stage: :string,
    method: :string,
    args: :string,
    deadline: :integer
  ]

  def run(task_name, argv) do
    {opts, args, _invalid} = OptionParser.parse(argv, strict: @switches)

    result =
      case task_name do
        "status" -> cmd_status(opts)
        "roster" -> cmd_roster(opts)
        "ps" -> cmd_ps(opts)
        "inspect" -> cmd_inspect(opts, args)
        "call" -> cmd_call(opts, args)
        "off" -> cmd_off(opts, args)
        "cues" -> cmd_cues(opts)
        "ring" -> cmd_ring(opts)
        other -> {:error, "unknown command #{inspect(other)}"}
      end

    case result do
      {:ok, data} ->
        if opts[:json] do
          IO.puts(JasonSafe.encode(data))
        else
          data |> Table.render() |> IO.puts()
        end

      {:table, rows} ->
        if opts[:json] do
          IO.puts(JasonSafe.encode(rows))
        else
          Table.render(rows) |> IO.puts()
        end

      {:raw, str} ->
        IO.puts(str)

      {:error, reason} ->
        IO.puts(:stderr, "error: #{format_err(reason)}")
        System.halt(1)
    end
  end

  # -- 命令 ------------------------------------------------------------------

  defp cmd_status(opts) do
    with_node(opts, :status, [], fn st ->
      {:table,
       [
         %{
           "troupe" => st.troupe,
           "stages" => st.stages,
           "on_stage actors" => st.on_stage,
           "cli node" => st.node
         }
       ]}
    end)
  end

  defp cmd_roster(opts) do
    with_node(opts, :roster, [], fn rows ->
      {:table,
       for r <- rows do
         %{"node" => r.node, "reachable" => r.reachable, "actors" => Map.get(r, :actors, "-")}
       end}
    end)
  end

  defp cmd_ps(opts) do
    filter = if opts[:role], do: [role: opts[:role]], else: []

    with_node(opts, :actors, [filter], fn actors ->
      {:table,
       for a <- actors do
         %{
           "role" => a.role_id,
           "stage_name" => a.stage_name,
           "node" => a.node,
           "state" => a.lifecycle,
           "idle_ms" => Map.get(a, :last_activity_ms) || "-",
           "queue" => Map.get(a, :pending, 0),
           "revision" => Map.get(a, :revision) || "-"
         }
       end}
    end)
  end

  defp cmd_inspect(opts, [role, stage]) do
    with_node(opts, :inspect, [role, stage], fn detail ->
      {:raw, inspect(detail, pretty: true, limit: 200)}
    end)
  end

  defp cmd_call(opts, [role, stage, method | rest]) do
    args =
      case rest do
        [] ->
          []

        [json] ->
          case JasonSafe.decode(json) do
            {:ok, list} when is_list(list) -> list
            _ -> raise "args must be a JSON array, got: #{inspect(json)}"
          end
      end

    with_node(opts, :invoke, [role, stage, String.to_atom(method), args], fn reply ->
      {:raw, inspect(reply, pretty: true, limit: 100)}
    end)
  end

  defp cmd_off(opts, [role, stage]) do
    with_node(opts, :off, [role, stage], fn _ -> {:ok, [{"off", "#{role}/#{stage}"}]} end)
  end

  defp cmd_cues(opts) do
    with_node(opts, :cues, [], fn cues ->
      {:table,
       for c <- cues do
         %{
           "role" => c.role_id,
           "stage_name" => c.stage_name,
           "cue" => c.name,
           "spec" => inspect(c.spec),
           "due_in_ms" => c.due_in_ms
         }
       end}
    end)
  end

  defp cmd_ring(opts) do
    with_node(opts, :ring, [], fn info ->
      {:table,
       for {n, vnodes} <- info.distribution do
         %{"node" => n, "vnodes" => vnodes, "share" => :erlang.float_to_binary(vnodes / 160, decimals: 2)}
       end}
    end)
  end

  # -- 连接 ------------------------------------------------------------------

  defp with_node(opts, cmd, args, fun) do
    troupe_name = str_to_atom(opts[:name] || default_troupe_name())

    result =
      case opts[:node] do
        nil ->
          if Troupe.by_name(troupe_name) do
            {:ok, Troupe.Director.api(troupe_name, cmd, args)}
          else
            {:error, "no troupe running locally (name: #{troupe_name})"}
          end

        node_str ->
          node = String.to_atom(node_str)

          if Node.alive?() do
            try do
              {:ok, :erpc.call(node, Troupe.Director, :api, [troupe_name, cmd, args])}
            catch
              :exit, {:erpc, reason} -> {:error, {:erpc, reason}}
              :error, {:erpc, reason} -> {:error, {:erpc, reason}}
              :exit, reason -> {:error, reason}
            end
          else
            {:error, "local node is not distributed — start with --sname/--name and matching cookie"}
          end
      end

    with {:ok, data} <- result, do: fun.(data)
  end

  defp default_troupe_name do
    :persistent_term.get({Troupe, :default}, :troupe)
  end

  defp str_to_atom(s) when is_binary(s), do: String.to_atom(s)
  defp str_to_atom(a) when is_atom(a), do: a

  defp format_err(reason) when is_binary(reason), do: reason
  defp format_err(reason), do: inspect(reason)
end

defmodule JasonSafe do
  @moduledoc false
  # 零依赖的 JSON 编码（仅用于 CLI 输出；结构不可编码时回退 inspect）

  def encode(term) do
    term |> to_jsonable() |> encode_value()
  rescue
    _ -> inspect(term)
  end

  def decode(bin) do
    case String.trim(bin) do
      "[" <> _ -> decode_list(bin)
      _ -> {:error, :not_a_list}
    end
  end

  defp decode_list(bin) do
    # CLI 场景的轻量解析：顶层逗号分隔的标量数组
    inner = bin |> String.trim() |> String.slice(1..-3//1)

    if String.trim(inner) == "" do
      {:ok, []}
    else
      items =
        inner
        |> split_top_level(",")
        |> Enum.map(fn item ->
          item = String.trim(item)

          cond do
            item == "true" -> true
            item == "false" -> false
            item == "null" -> nil
            String.match?(item, ~r/^-?\d+$/) -> String.to_integer(item)
            String.match?(item, ~r/^-?\d+\.\d+$/) -> String.to_float(item)
            String.starts_with?(item, "\"") -> String.slice(item, 1..-2//1)
            true -> item
          end
        end)

      {:ok, items}
    end
  end

  defp split_top_level(bin, sep) do
    String.split(bin, sep)
  end

  defp to_jsonable(v) when is_map(v), do: Map.new(v, fn {k, x} -> {to_string(k), to_jsonable(x)} end)
  defp to_jsonable(v) when is_list(v), do: Enum.map(v, &to_jsonable/1)
  defp to_jsonable(v) when is_atom(v) and not is_boolean(v), do: to_string(v)
  defp to_jsonable(v) when is_tuple(v), do: to_string(inspect(v))
  defp to_jsonable(v) when is_pid(v), do: inspect(v)
  defp to_jsonable(v) when is_reference(v), do: inspect(v)
  defp to_jsonable(v) when is_function(v), do: inspect(v)
  defp to_jsonable(v), do: v

  defp encode_value(v) when is_binary(v), do: ~s("#{escape(v)}")
  defp encode_value(v) when is_number(v), do: to_string(v)
  defp encode_value(true), do: "true"
  defp encode_value(false), do: "false"
  defp encode_value(nil), do: "null"
  defp encode_value(v) when is_list(v), do: "[" <> Enum.map_join(v, ",", &encode_value/1) <> "]"

  defp encode_value(v), do: ~s("#{escape(inspect(v))}")

  defp escape(s), do: String.replace(s, "\"", "\\\"")
end

defmodule Table do
  @moduledoc false

  def render(list) when is_list(list) and list != [] do
    keys = list |> Enum.flat_map(&Map.keys/1) |> Enum.uniq()

    rows = for m <- list, do: for(k <- keys, do: cell(Map.get(m, k)))

    widths =
      for {k, i} <- Enum.with_index(keys) do
        cells = for r <- rows, do: String.length(Enum.at(r, i))
        max(String.length(k), cells |> Enum.max(&>=/2, fn -> 0 end))
      end

    header = Enum.map_join(Enum.zip(keys, widths), " | ", fn {k, w} -> String.pad_trailing(k, w) end)
    sep = Enum.map_join(widths, "-+-", &String.duplicate("-", &1))

    body =
      for r <- rows do
        Enum.map_join(Enum.with_index(r), " | ", fn {c, i} -> String.pad_trailing(c, Enum.at(widths, i)) end)
      end

    Enum.join([header, sep | body], "\n")
  end

  def render(_), do: "(empty)"

  defp cell(v) when is_binary(v), do: v
  defp cell(v), do: inspect(v)
end
