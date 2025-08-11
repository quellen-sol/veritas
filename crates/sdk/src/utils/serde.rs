use petgraph::graph::EdgeIndex;
use serde::Serializer;

pub fn serialize_edge_index<S: Serializer>(
    edge_index: &EdgeIndex,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    serializer.serialize_u64(edge_index.index() as u64)
}
