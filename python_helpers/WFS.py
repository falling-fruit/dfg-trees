class WFS:
    def __init__(self, featuresUrl: str, capabilitiesUrl: str, version: str, pagingEnabled: bool, hitsEnabled: bool) -> None:
        self.featuresUrl = featuresUrl
        self.capabilitiesUrl = capabilitiesUrl
        self.version = version
        self.pagingEnabled = pagingEnabled
        self.hitsEnabled = hitsEnabled
    

    def getFeaturesUrl(self):
        return self.featuresUrl
    
    def getCapabilitiesUrl(self):
        return self.capabilitiesUrl
    
    def getVersion(self):
        return self.version
    
    def isPagingEnabled(self):
        return self.pagingEnabled
    
    def isHitsEnabled(self):
        return self.hitsEnabled
