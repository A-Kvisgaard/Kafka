from opcua import Client, ua
native_var_names = ['ServerArray', 'NamespaceArray', 'ServerStatus', 'ServiceLevel', 'Auditing',
                                 'EstimatedReturnTime', 'ServerProfileArray', 'LocaleIdArray', 'MinSupportedSampleRate',
                                 'MaxBrowseContinuationPoints', 'MaxQueryContinuationPoints',
                                 'MaxHistoryContinuationPoints', 'SoftwareCertificates', 'MaxArrayLength',
                                 'MaxStringLength', 'MaxByteStringLength', 'MaxNodesPerRead',
                                 'MaxNodesPerHistoryReadData', 'MaxNodesPerHistoryReadEvents', 'MaxNodesPerWrite',
                                 'MaxNodesPerHistoryUpdateData', 'MaxNodesPerHistoryUpdateEvents',
                                 'MaxNodesPerMethodCall', 'MaxNodesPerBrowse', 'MaxNodesPerRegisterNodes',
                                 'MaxNodesPerTranslateBrowsePathsToNodeIds', 'MaxNodesPerNodeManagement',
                                 'MaxMonitoredItemsPerCall', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'Identities', 'Applications', 'ApplicationsExclude',
                                 'Endpoints', 'EndpointsExclude', 'AccessHistoryDataCapability',
                                 'AccessHistoryEventsCapability', 'MaxReturnDataValues', 'MaxReturnEventValues',
                                 'InsertDataCapability', 'ReplaceDataCapability', 'UpdateDataCapability',
                                 'DeleteRawCapability', 'DeleteAtTimeCapability', 'InsertEventCapability',
                                 'ReplaceEventCapability', 'UpdateEventCapability', 'DeleteEventCapability',
                                 'InsertAnnotationCapability', 'ServerDiagnosticsSummary',
                                 'SamplingIntervalDiagnosticsArray', 'SubscriptionDiagnosticsArray',
                                 'SessionDiagnosticsArray', 'SessionSecurityDiagnosticsArray', 'EnabledFlag',
                                 'RedundancySupport', 'CurrentServerId', 'RedundantServerArray', 'ServerUriArray',
                                 'ServerNetworkGroups', 'NamespaceUri', 'NamespaceVersion', 'NamespacePublicationDate',
                                 'IsNamespaceSubset', 'StaticNodeIdTypes', 'StaticNumericNodeIdRange',
                                 'StaticStringNodeIdPattern', 'DefaultRolePermissions', 'DefaultUserRolePermissions',
                                 'DefaultAccessRestrictions', 'CurrentTimeZone', 'Opc.Ua', 'Opc.Ua']
client = None
nodes = []
def assert_variable(name):
    return name not in native_var_names

def browse_recursive(node):
    # browse through all children of current node
    for child_id in node.get_children():
        # retrieve the node based on the id of the current child
        child = client.get_node(child_id)
        # an object contains variables and has no values, thus the object is recursed
        if child.get_node_class() == ua.NodeClass.Object:
                browse_recursive(child)
        # if child is a variable perform program logic
        elif child.get_node_class() == ua.NodeClass.Variable:

            # extracting ua qualified name
            browse_name = child.get_browse_name().Name
            if assert_variable(browse_name):
                nodes.append(browse_name)
                num_children = len(child.get_children())
                if(num_children > 0):
                    print(str(num_children) + "  " + browse_name)
                    browse_recursive(child)
    
if __name__ == "__main__":
    try:
        client = Client("opc.tcp://tek-matrikon0b.tek.c.sdu.dk:48400/UA/NETxBMSPlatform")
        client.connect()    
        root_node = client.get_root_node()
        browse_recursive(root_node)
        print(len(nodes))
    finally:
        client.disconnect()
