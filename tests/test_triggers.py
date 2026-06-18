from kairos_macro.triggers import ShockDetector


def test_price_crash_fires_beyond_threshold():
    d = ShockDetector(crash_pct_1h=10.0)
    assert d.check_price(-12.0) is not None
    assert d.check_price(-5.0) is None
    assert d.check_price(8.0) is None


def test_macro_surprise_fires_on_2sigma():
    d = ShockDetector()
    assert d.check_macro(indicator="CPI", surprise_sigma=2.5) is not None
    assert d.check_macro(indicator="CPI", surprise_sigma=1.0) is None
