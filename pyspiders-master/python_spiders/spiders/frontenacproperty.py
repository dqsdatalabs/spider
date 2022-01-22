import scrapy
import dateutil.parser
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces
import requests
class FrontenacpropertySpider(scrapy.Spider):
    name = 'frontenacproperty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['frontenacproperty.com']
    
    def start_requests(self):
        start_urls = [            
            {'url': 'https://www.frontenacproperty.com/properties/family/?sort=availability&order=ASC',
                'property_type': 'house'},
            {'url': 'https://www.frontenacproperty.com/properties/condo-apartment/?sort=availability&order=ASC',
                'property_type': 'apartment'},
            {'url': 'https://www.frontenacproperty.com/properties/stud-rentals/',
                'property_type': 'apartment'}
            ]
        for url in start_urls:
            yield scrapy.Request(url=url.get('url'), callback=self.parse, meta={'property_type': url.get('property_type')})
            
            
    def parse(self, response):
        for ad in response.css(".property-img a::attr(href)").getall():
            yield scrapy.Request(url=ad, callback=self.parse_page, meta={'property_type':response.meta.get('property_type')})
    
    def parse_page(self, response):
        property_type = response.meta['property_type']
        images = response.css("ul.slides li img::attr(src)").getall()
        title = response.css("h2.single-title.entry-title::text").get()
        features = response.css(".pull-left span.property-icon::attr(title)").getall()
        available_date = response.css(".entry-content h4 b::text").get().replace("Availability:","").strip()
        rent = response.css("span.property-price b::text").get().replace("$","")
        description = remove_white_spaces(" ".join(response.css(".entry-content ul li::text").getall()))
        address = response.css("h2.single-title.entry-title::text").get().split(",")[0]
        landlord_name = response.css("meta[name='twitter:data1']::attr(content)").get()
        latitude = response.css("div.marker::attr(data-lat)").get()
        longitude = response.css("div.marker::attr(data-lng)").get()

        responseGeocode = requests.get(f"https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode?location={longitude},{latitude}&f=pjson&distance=50000&outSR=")

        responseGeocodeData = responseGeocode.json()

        zipcode = responseGeocodeData['address']['Postal']
        
        city = responseGeocodeData['address']['City']
        

        if response.css("span.bedrooms::text").get()=='Bachelor':
            room_count = 1
            bathroom_count = int(float(response.css("span.bedrooms b::text").getall()[0]))
        else:
            if len(response.css("span.bedrooms b::text").getall()) > 1:
                room_count = int(float(response.css("span.bedrooms b::text").get()))
                bathroom_count = int(float(response.css("span.bedrooms b::text").getall()[1]))
            else:
                room_count = int(float(response.css("span.bedrooms b::text").get()))

        
        if rent:
            rent = int(rent.strip())
        parking, washing_machine = '',''
        if "Has Parking" in features:
            parking = True
        elif "No Parking" in features:
            parking = False
        if "Has Laundry" in features:
            washing_machine = True
        
        elif 'No Laundry' in features:
            washing_machine = False
        
        
        if available_date != 'RENTED':
        
            if available_date == "Immediate":
                available_date = ''
            else:
                if available_date:
                    available_date = dateutil.parser.parse(available_date).strftime("%Y-%m-%d")

            item = ListingLoader(response=response)
            item.add_value("external_source"        ,self.external_source)
            item.add_value("address"                ,address)
            item.add_value("external_link"          ,response.url)
            item.add_value("currency"               ,"CAD")
            item.add_value("images"                 ,images)
            item.add_value("title"                  ,title)
            item.add_value("rent"                   ,rent)
            item.add_value("washing_machine"        ,washing_machine)
            item.add_value("room_count"             ,room_count)
            item.add_value("bathroom_count"         ,bathroom_count)
            item.add_value("property_type"          ,property_type)
            item.add_value("parking"                ,parking)
            item.add_value("available_date"         ,available_date)
            item.add_value("description"            ,description)
            item.add_value("landlord_name"          ,landlord_name)
            item.add_value("latitude"               ,latitude)
            item.add_value("longitude"              ,longitude)
            item.add_value("zipcode"                ,zipcode)
            item.add_value("city"                   ,city)



            yield item.load_item()
