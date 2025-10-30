from duet_screen.consensus import weighted_average_rank, weighted_reciprocal_rank_fusion


def test_weighted_average_rank_basic():
    ranks = [
        ["A", "B", "C"],
        ["B", "A", "C"],
    ]
    weights = [0.7, 0.3]
    result = weighted_average_rank(ranks, weights)
    assert result["A"] == 0.7 * 1 + 0.3 * 2
    assert result["B"] == 0.7 * 2 + 0.3 * 1
    assert result["C"] == 0.7 * 3 + 0.3 * 3


def test_weighted_reciprocal_rank_fusion_orders_descending():
    ranks = [
        ["X", "Y", "Z"],
        ["Y", "X", "Z"],
    ]
    weights = [0.5, 0.5]
    fused = weighted_reciprocal_rank_fusion(ranks, weights, constant=10)
    items = list(fused.keys())
    assert items[0] == "X"
    assert fused["X"] == fused["Y"]
    assert fused["X"] > fused["Z"]
