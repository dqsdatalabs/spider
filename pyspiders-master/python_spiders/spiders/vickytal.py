import scrapy
import json
from python_spiders.loaders import ListingLoader

class VickytalSpider(scrapy.Spider):
    name = 'vickytal'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    
    allowed_domains = ['vickytal.com']
    start_urls = ['https://vickytal.com/WebService.svc/SearchListingsAdapter?fwdId=5893dbfb455f89286c86b73d&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.65771%2C%22Longitude%22%3A-79.38618%2C%22BoundsNorth%22%3A90%2C%22BoundsSouth%22%3A-90%2C%22BoundsEast%22%3A180%2C%22BoundsWest%22%3A-180%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Atrue%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D']

    def parse(self, response):
        data = json.loads(response.body)
        
        for items in data['results']:
            item = ListingLoader(response=response)
            
            item.add_value("external_source",            self.external_source)
            item.add_value("city",                       items['addressDetails']['city'])
            item.add_value("images",                     items['images'])
            item.add_value("latitude",                   str(items['latitude']))
            item.add_value("longitude",                  str(items['longitude']))
            item.add_value("property_type",              make_prop(items['propertyTypeName']))
            item.add_value("landlord_email",             "victoriatal@rogers.com")
            item.add_value("landlord_phone",             "(416)-363-3473")
            item.add_value("currency",                   "CAD")
            if 'beds' in items.keys():
                item.add_value("room_count",             items['beds'])                
            if 'baths' in items.keys():
                item.add_value("bathroom_count",         items['baths'])
            if 'address' in items.keys():
                item.add_value("address",                items['address'])
            if 'zip' in items['addressDetails'].keys():
                item.add_value("zipcode",                items['addressDetails']['zip'])
            if 'description' in items.keys():
                item.add_value("description",            items['description'])
            if 'mlNum' in items.keys():
                item.add_value("external_id",            items['mlNum'])
                item.add_value("external_link",          "https://vickytal.com/Listing/"+items['mlNum']+"?id="+str(items['listingId']))
            if 'parkingSpaces' in items.keys():
                item.add_value("parking",                items['parkingSpaces'] > 0)
            if 'sqft' in items.keys():
                item.add_value("square_meters",          int(int(make_square(items['sqft']))*10.764))
            if 'listPrice' in items.keys():
                item.add_value("rent",                   int(items['listPrice']))
                yield item.load_item()


            
def make_prop(prop):
    prop = prop.lower()
    if prop in ['condo']:
        return 'apartment'
    return prop

def make_square(square):
    max = square.split("-")
    min = int(max[0])
    max = int(max[-1])
    
    if min == 0:
        return round(max*0.0929)
    return round(((max+min)/2)*0.0929)
    
