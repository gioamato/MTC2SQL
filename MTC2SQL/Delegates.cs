using System.Collections.Generic;
using TrakHound.Api.v2.Streams.Data;

namespace MTC2SQL
{
    public delegate void AgentDefinitionsHandler(AgentDefinitionData definition);

    public delegate void AssetDefinitionsHandler(List<AssetDefinitionData> definitions);

    public delegate void ComponentDefinitionsHandler(List<ComponentDefinitionData> definitions);

    public delegate void DataItemDefinitionsHandler(List<DataItemDefinitionData> definitions);

    public delegate void DeviceDefinitionsHandler(DeviceDefinitionData definition);

    public delegate void SamplesHandler(List<SampleData> samples);

    public delegate void StatusHandler(StatusData status);
}