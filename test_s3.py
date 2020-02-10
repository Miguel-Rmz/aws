import s3


class TestS3LocalFuncs():
    def test_s3_target_directory(self):
        # -- True
        assert s3.is_target_directory(key='error.txt', prefix_filter='', delimiter='/') is True
        assert s3.is_target_directory(key='mramirez/development/success.txt', prefix_filter='mramirez/development/', delimiter='/') is True
        assert s3.is_target_directory(key='mramirez/development/success.txt', prefix_filter='mramirez/development/', delimiter='/') is True
        assert s3.is_target_directory(key='mramirez/development/', prefix_filter='mramirez/development/', delimiter='/') is True
        assert s3.is_target_directory(key='mramirez/development/', prefix_filter='mramirez/development', delimiter='/') is True
        # -- False
        assert s3.is_target_directory(key='Test/error.txt', prefix_filter='', delimiter='/') is False
        assert s3.is_target_directory(key='Test/Test2/Test3/error.txt', prefix_filter='', delimiter='/') is False
        assert s3.is_target_directory(key='Test/Test2/Test3/error.txt', prefix_filter='Test/Test2/', delimiter='/') is False
        assert s3.is_target_directory(key='Test/Test2/Test3/error.txt', prefix_filter='Test', delimiter='/') is False
        assert s3.is_target_directory(key='mramirez/development/', prefix_filter='mramirez/', delimiter='/') is False
        assert s3.is_target_directory(key='Test/Test/test2.txt', prefix_filter='', delimiter='/') is False
