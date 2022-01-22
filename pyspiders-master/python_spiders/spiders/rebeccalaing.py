import scrapy
import json
from python_spiders.loaders import ListingLoader
import re
from python_spiders.helper import sq_feet_to_meters



class RebeccalaingSpider(scrapy.Spider):
    name = 'rebeccalaing'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['rebeccalaing.ca']
    start_urls = ['https://rebeccalaing.ca/WebService.svc/SearchListingsAdapter?fwdId=5d9f8ebad0d6d912245e3037&model=%7B%22IsCommunity%22%3Atrue%2C%22Latitude%22%3A43.67135%2C%22Longitude%22%3A-79.29281%2C%22BoundsNorth%22%3A90%2C%22BoundsSouth%22%3A-90%2C%22BoundsEast%22%3A180%2C%22BoundsWest%22%3A-180%2C%22Pivot%22%3A%224%22%2C%22MinPrice%22%3A%22Any%22%2C%22MaxPrice%22%3A%22Any%22%2C%22Beds%22%3A%220%22%2C%22Baths%22%3A%220%22%2C%22BuildingType%22%3A0%2C%22ShowIDX%22%3Afalse%2C%22Proximity%22%3Atrue%2C%22Source%22%3A0%2C%22Query%22%3A%22%22%7D']

    def parse(self, response):
        data = json.loads(response.body)['results']
        
        for ad in data:
            if ad['saleLease'] == "Lease" and ad["status"] == "Available" and 'address' in ad.keys():
                    
                if 'mlNum' not in ad.keys():
                    temp = re.findall("E[0-9]+-",str(ad))
                    
                    if len(temp) > 0:
                        external_id = temp[0].replace("-","")
                else:
                    external_id = ad['mlNum']
                
                external_link = "https://rebeccalaing.ca/listing/" + ad['address'].replace(",","").lower().replace(" ","-")+'-'+external_id+"?id="+str(ad['listingId'])
                
                item = ListingLoader(response=response)
                if 'beds' in ad.keys():
                    if ad['beds'] != 0:
                        item.add_value('bathroom_count'         , ad['baths'])
                        square_meters = ''
                        if 'sqft' in ad.keys():
                            if '-' in ad['sqft']:
                                square_meters = sq_feet_to_meters(round(eval(ad['sqft'].replace("-","+"))/2))
                            elif '<' in ad['sqft']:
                                square_meters = sq_feet_to_meters(int(ad['sqft'].replace("<","").strip()))




                        item.add_value("external_source"        , self.external_source)
                        item.add_value("external_link"          , external_link)
                        item.add_value("external_id"            , external_id)
                        item.add_value('address'                , ad['addressDetails']['formattedStreetAddress'])
                        item.add_value('city'                   , ad['addressDetails']['city'])
                        item.add_value('zipcode'                , ad['addressDetails']['zip'])
                        item.add_value('room_count'             , ad['beds'])
                        item.add_value('latitude'               , str(ad['latitude']))
                        item.add_value('longitude'              , str(ad['longitude']))
                        item.add_value('rent'                   , int(ad['listPrice']))
                        item.add_value('currency'               , "CAD")
                        item.add_value('parking'                , ad['parkingSpaces']!= 0)
                        item.add_value('property_type'          , make_prop(ad['styles']))
                        item.add_value('description'            , ad['description'])
                        item.add_value('images'                 , ad['images'])
                        item.add_value('square_meters'          , int(int(square_meters)*10.764))

                        if 'Washer' in ad['description']:
                            item.add_value('washing_machine'          , True)
                        if 'Dishwasher' in ad['description']:
                            item.add_value('dishwasher'               , True)

                        yield scrapy.Request(url=external_link, callback=self.parse_page, meta={"item":item})

                
    def parse_page(self, response):
        item = response.meta['item']
        furnished   = response.css("listing-details").re(r'isFurnished": [a-zA-Z]+')
        swimming_pool   = response.css("listing-details").re(r'hasPool": [a-zA-Z]+')

        if len(furnished):
            furnished = furnished[0].split(":")[-1].strip()
            furnished = furnished != 'false'

        if len(swimming_pool):
            swimming_pool = swimming_pool[0].split(":")[-1].strip()
            swimming_pool = swimming_pool != 'false'

        title = response.css("meta[property='og:title']::attr(content)").get()
        item.add_value("furnished"              ,furnished)
        item.add_value("swimming_pool"              ,swimming_pool)
        item.add_value("title"                  ,title)
        item.add_value("landlord_name"          ,'Rebecca Laing')
        item.add_value("landlord_phone"         ,'416-357-1059')
        item.add_value("landlord_email"         ,'info@rebeccalaing.ca')
        yield item.load_item()
        
        
def make_prop(val):
    for i in val:
        if i['name'].lower() in ['detached', 'house', 'twnhouse','bungalow','multi-level']:
            return 'house'
        elif i['name'].lower() in ['apartment', 'condo', '2-storey','fourplex', 'condo apt', '3-storey', 'condo townhouse', 'co-op apt','loft','bungaloft','2 1/2 storey']:
            return 'apartment'
