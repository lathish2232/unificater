AdminTask.createApplicationServer('localhostNode01', '[-name server10 -templateName default -genUniquePorts true ]')

AdminConfig.save()
AdminControl.startServer('serve10', 'localhostNode01')

AdminTask.createJDBCProvider('[-scope Cell=localhostCell01 -databaseType Oracle -providerType "Oracle JDBC Driver" -implementationType "Connection pool data source" -name Oracle JDBC Driver123 -description "Oracle JDBC Driver" -classpath [${ORACLE_JDBC_DRIVER_PATH}/ojdbc6.jar ] -nativePath "" ]')

AdminConfig.save()

AdminTask.createDatasource('"Oracle Oracle JDBC Driver123(cells/localhostCell01|resources.xml#JDBCProvider_1596681947508)"', '[-name DS_AK -jndiName jdbc/DS_AK -dataStoreHelperClassName com.ibm.websphere.rsadapter.Oracle11gDataStoreHelper -containerManagedPersistence true -componentManagedAuthenticationAlias localhostCellManager01/userdet12 -configureResourceProperties [[URL java.lang.String jdbc:oracle:thin:@192.168.190.1:1521:xe]]]')

AdminConfig.save()