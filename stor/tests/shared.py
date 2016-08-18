
def assert_same_data(fp1, fp2):
    actual_data = fp1.read(100)
    expected_data = fp2.read(100)
    while (expected_data or actual_data):
        assert actual_data == expected_data
        actual_data = fp1.read(100)
        expected_data = fp2.read(100)
