using System.Data.OracleClient.Oci;

namespace System.Data.OracleClient {

internal struct OracleConnectionInfo
	{
		internal string Username;
		internal string Password;
		internal string Database;
		internal string ConnectionString;
		internal OciCredentialType CredentialType;
		internal bool SetNewPassword;
		internal string NewPassword;
	}
}