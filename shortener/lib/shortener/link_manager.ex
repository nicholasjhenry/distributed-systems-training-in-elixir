defmodule Shortener.LinkManager do
  @moduledoc """
  Manages the lifecycles of links
  """

  alias Shortener.Storage
  alias Shortener.LinkManager.Cache
  alias Shortener.Cluster

  @lookup_sup __MODULE__.LookupSupervisor

  def child_spec(_args) do
    children = [
      Cache,
      # Extend this supervision tree to support remote lookups
      {Task.Supervisor, name: @lookup_sup}
    ]

    %{
      id: __MODULE__,
      type: :supervisor,
      start: {Supervisor, :start_link, [children, [strategy: :one_for_one]]}
    }
  end

  def create(url) do
    short_code = generate_short_code(url)
    :ok = Storage.set(Storage, short_code, url)
    node = Cluster.find_node(short_code) |> IO.inspect(label: ">>>>")
    :ok = Cache.insert({Cache, node}, short_code, url)

    {:ok, short_code}
  catch
    :exit, _ ->
      {:error, :node_down}
  end

  def lookup(short_code) do
    case Cache.lookup(short_code) do
      {:error, :not_found} ->
        get_and_cache(short_code)
      result -> result
    end
  end

  defp get_and_cache(short_code) do
    {:ok, url} = Storage.get(short_code)
    :ok = Cache.insert(short_code, url)
    {:ok, url}
  end

  def remote_lookup(short_code) do
    node = Cluster.find_node(short_code)

    # Do a remote lookup
    Task.Supervisor.async(
    {@lookup_sup, node},
      __MODULE__,
      :lookup,
      [short_code]
    )
    |> Task.await()

  catch
    :exit, _ ->
      {:error, :node_down}
  end

  def generate_short_code(url) do
    url
    |> hash
    |> Base.encode16(case: :lower)
    |> String.to_integer(16)
    |> pack_bitstring
    |> Base.url_encode64()
    |> String.replace(~r/==\n?/, "")
  end

  defp hash(str), do: :crypto.hash(:sha256, str)

  defp pack_bitstring(int), do: <<int::big-unsigned-32>>
end
