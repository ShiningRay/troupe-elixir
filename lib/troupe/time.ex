defmodule Troupe.Time do
  @moduledoc """
  时长解析与系统时间辅助。

  时长接受整数毫秒或字符串（`"100"`、`"2s"`、`"5m"`、`"1h"`、`"7d"`），
  与 Troupe.js 的 duration 语义一致。
  """

  @spec parse(integer() | String.t()) :: non_neg_integer()
  def parse(ms) when is_integer(ms) and ms >= 0, do: ms

  def parse(bin) when is_binary(bin) do
    case Regex.run(~r/^(\d+)(ms|s|m|h|d)?$/, String.trim(bin)) do
      [_, num] -> String.to_integer(num)
      [_, num, unit] -> String.to_integer(num) * unit_ms(unit)
      nil -> raise ArgumentError, "invalid duration: #{inspect(bin)}"
    end
  end

  def parse(other), do: raise(ArgumentError, "invalid duration: #{inspect(other)}")

  defp unit_ms(nil), do: 1
  defp unit_ms("ms"), do: 1
  defp unit_ms("s"), do: 1_000
  defp unit_ms("m"), do: 60_000
  defp unit_ms("h"), do: 3_600_000
  defp unit_ms("d"), do: 86_400_000

  @doc "当前墙钟毫秒（deadline 用绝对时间，跨 Stage 可传播）。"
  @spec now_ms() :: integer()
  def now_ms, do: System.system_time(:millisecond)

  @doc "格式化毫秒为人类可读（日志/CLI）。"
  @spec human(integer()) :: String.t()
  def human(ms) when ms < 1_000, do: "#{ms}ms"
  def human(ms) when ms < 60_000, do: :erlang.float_to_binary(ms / 1_000, decimals: 1) <> "s"
  def human(ms) when ms < 3_600_000, do: :erlang.float_to_binary(ms / 60_000, decimals: 1) <> "m"
  def human(ms), do: :erlang.float_to_binary(ms / 3_600_000, decimals: 1) <> "h"
end
