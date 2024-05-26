using System.Collections.Generic;
using TableHunt.Models;

namespace TableHunt;

/// <summary>
/// For performing queries
///
/// Will move ths out of code eventially.
/// </summary>
public static class Data
{
    public static List<DataQuery> Queries = new List<DataQuery>()
    {
        new DataQuery()
        {
            Name="Effectiveness",
            Index="Date_value",
            Query = @"let QueryTime = 30d;
let Reports = CloudAppEvents  
| where Timestamp > ago(QueryTime)
| where ActionType == ""UserSubmission"" or ActionType == ""AdminSubmission""
| extend MessageDate = todatetime((parse_json(RawEventData)).MessageDate)
| extend NetworkMessageID = tostring((parse_json(RawEventData)).ObjectId)
| extend Date_value = tostring(format_datetime( MessageDate, ""yyyy-MM-dd""))
| extend ExtractVerdict= tostring((parse_json(RawEventData)).DeliveryMessageInfo)
| extend OriginalVerdict = tostring((parse_json(ExtractVerdict)).FinalVerdict)
| extend RescanVerdictSubmission= tostring((parse_json(RawEventData)).RescanResult.RescanVerdict)
| summarize MessagesGotReported = count(),
        PostAllow = countif(RescanVerdictSubmission contains ""Allow""),
        PostNotSpam = countif(RescanVerdictSubmission contains ""NotSpam""),
        PostSpam = countif(RescanVerdictSubmission contains ""Spam""),
        PostBulk = countif(RescanVerdictSubmission contains ""Bulk""),
        PostBlock = countif(RescanVerdictSubmission contains ""Block""),
        PostPhish = countif(RescanVerdictSubmission contains ""Phish""),
        PostMalware = countif(RescanVerdictSubmission contains ""Malware""),
        PreAllow = countif(OriginalVerdict contains ""Allow""),
        PreNotSpam = countif(OriginalVerdict contains ""NotSpam""),
        PreSpam = countif(OriginalVerdict contains ""Spam""),
        PreBulk = countif(OriginalVerdict contains ""Bulk""),
        PreBlock = countif(OriginalVerdict contains ""Block""),
        PrePhish = countif(OriginalVerdict contains ""Phish""),
        PreMalware = countif(OriginalVerdict contains ""Malware"")        
        by Date_value
| project Date_value, PreAllow, PreNotSpam, PreSpam, PostBulk, PreBlock, PrePhish, PreMalware, MessagesGotReported, PostAllow, PostNotSpam, PostSpam, PostBlock, PostPhish, PostMalware;
let ThreatByAutomation = (AlertEvidence | where Title == ""Email reported by user as malware or phish"")
| extend LastVerdictfromAutomation = tostring((parse_json(AdditionalFields)).LastVerdict)
| extend Date_value = tostring(format_datetime( Timestamp, ""yyyy-MM-dd""))
| extend DetectionFromAIR = iif(isempty(LastVerdictfromAutomation), ""NoThreatsFound"", tostring(LastVerdictfromAutomation))
| summarize PostDeliveryTotalAIRInvestigations = count(),
            PostDeliveryAirNoThreatsFound = countif(DetectionFromAIR contains ""NoThreatsFound""),
            PostDeliveryAirSuspicious = countif(DetectionFromAIR contains ""Suspicious""),
            PostDeliveryAirMalicious = countif(DetectionFromAIR contains ""Malicious"")
            by Date_value
| project Date_value, PostDeliveryTotalAIRInvestigations, PostDeliveryAirNoThreatsFound, PostDeliveryAirSuspicious, PostDeliveryAirMalicious;
let DeliveryInboundEvents = (EmailEvents | where EmailDirection == ""Inbound"" and Timestamp > ago(QueryTime)
| extend Date_value = tostring(format_datetime( Timestamp, ""yyyy-MM-dd""))
| project Date_value, Timestamp, NetworkMessageId, DetectionMethods ,RecipientEmailAddress);
let PostDeliveryEvents = (EmailPostDeliveryEvents | where ActionType contains ""ZAP"" and ActionResult == ""Success""| join DeliveryInboundEvents on RecipientEmailAddress, NetworkMessageId
| summarize PostDeliveryZAP=count() by Date_value);
let DeliveryByThreat = (DeliveryInboundEvents
| where Timestamp > ago(QueryTime)
| extend Date_value = tostring(format_datetime( Timestamp, ""yyyy-MM-dd""))
| extend MDO_detection = parse_json(DetectionMethods)
| extend FirstDetection = iif(isempty(MDO_detection), ""Clean"", tostring(bag_keys(MDO_detection)[0]))
| extend FirstSubcategory = iif(FirstDetection != ""Clean"" and array_length(MDO_detection[FirstDetection]) > 0, strcat(FirstDetection, "": "", tostring(MDO_detection[FirstDetection][0])), ""No Detection (clean)""))
| summarize TotalEmails = count(),
            Clean = countif(FirstSubcategory contains ""Clean""),
            Malware = countif(FirstSubcategory contains ""Malware""),
            Phish = countif(FirstSubcategory contains ""Phish""),
            Spam = countif(FirstSubcategory contains ""Spam"" and FirstSubcategory !contains ""Bulk""),
            Bulk = countif(FirstSubcategory contains ""Bulk"")                  
            by Date_value;
DeliveryByThreat
| join kind=fullouter Reports on Date_value
| join kind=fullouter PostDeliveryEvents on Date_value
| join kind=fullouter ThreatByAutomation on Date_value
| sort by Date_value asc
| project Date_value, Clean, Malware, Phish, Spam, Bulk, MessagesGotReported, PostDeliveryZAP, PreAllow, PreNotSpam, PreSpam, PreBlock, PrePhish, PreMalware, PostAllow, PostNotSpam, PostSpam, PostBulk, PostBlock, PostPhish, PostMalware
| where isnotempty(Date_value)",
            
        }
        
    };
}