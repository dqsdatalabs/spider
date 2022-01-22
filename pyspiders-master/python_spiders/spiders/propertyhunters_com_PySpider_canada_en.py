import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math

class propertyhunters_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'propertyhunters_com'
    allowed_domains = ['propertyhunters.com','app.tenantturner.com']
    start_urls = [
        'https://propertyhunters.com/rentals/'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        yield Request(url='https://app.tenantturner.com/listings-json/13255',
                    callback=self.parse,
                    body='',
                    method='GET')

    def parse(self, response):  
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            external_id = item['id']
            latitude = str(item['latitude'])
            longitude = str(item['longitude'])
            address = item['address']
            city = item['city']
            zipcode = item['zip']
            title = item['title']
            description = item['description']
            beds = item['beds']
            baths = item['baths']
            dateAvailable = item['dateAvailable']
            rent = item['rentAmount']
            url = item['btnUrl']
            property_type =item['propertyType']
            yield Request(url=url, callback=self.parse_property, 
            meta={'external_id':external_id,
                'latitude':latitude,
                'longitude':longitude,
                'title':title,
                'address':address,
                'city':city,
                'zipcode':zipcode,
                'description':description,
                'beds':beds,
                'baths':baths,
                'dateAvailable':dateAvailable,
                'rent':rent,
                'property_type':property_type})


    def parse_property(self, response):
        description_1 = response.meta.get("description")
        if "Commercial" not in description_1:
            item_loader = ListingLoader(response=response)
            title = response.meta.get("title")
            latitude = response.meta.get("latitude")
            longitude = response.meta.get("longitude")
            city = response.meta.get("city")
            zipcode = response.meta.get("zipcode")
            address = response.meta.get("address")
            room_count = int(response.meta.get("beds"))
            bathroom_count = response.meta.get("baths")
            rent = response.meta.get("rent")
            available_date = response.meta.get("dateAvailable")
            property_type =str(response.meta.get("property_type")).lower()
            if "apartment" in property_type or "condo" in property_type or "single" in property_type or "none" in property_type:
                property_type = 'apartment'
            else:
                property_type = 'house'
            external_id = response.meta.get("external_id")
            description = description_1
            if "Contact Property Hunters Inc. office:  519-944-7368 or call our automated online booking at: 709-909-0494" in description:
                description = description.replace('Contact Property Hunters Inc. office:  519-944-7368 or call our automated online booking at: 709-909-0494','')
            if "Contact Property Hunters today! 519-944-7368" in description:
                description = description.replace('Contact Property Hunters today! 519-944-7368','')
            if "TRY OUR NEW AUTOMATED ONLINE LEASING AGENT TO BOOK YOUR SHOWING 709-909-0494" in description:
                description = description.replace('TRY OUR NEW AUTOMATED ONLINE LEASING AGENT TO BOOK YOUR SHOWING 709-909-0494','')
            if ".0" in bathroom_count:
                bathroom_count = int(bathroom_count.replace('.0',''))
            else:
                bathroom_count = int(math.ceil(float(bathroom_count)))
            images = response.css("img.rsImg::attr(src)").extract()
            external_images_count = len(images)
            info = response.css("table > tbody > tr").extract()
            deposit = None
            washing_machine = None
            dishwasher = None
            parking = None
            amenities = None
            for i in range(len(info)):
                if "Deposit" in info[i]:
                    deposit = info[i].split("$")[1].split("<")[0]
                    deposit = int(deposit.replace(",","")) 
                if "Amenities" in info[i]:
                    amenities = info[i].split("<td>")[1].split("<")[0]
            if "laundry" in description:
                washing_machine = True
            if "dishwasher" in description: 
                dishwasher = True
            try:
                if "dishwasher" in amenities:
                    dishwasher = True
                if "parking" in amenities:
                    parking = True
            except:
                pass
            if "parking" in description or "garage" in description: 
                parking = True
            

            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_id',external_id)
            item_loader.add_value('external_source',self.external_source)
            item_loader.add_value('title',title)
            item_loader.add_value('description',description)
            item_loader.add_value('city',city)
            item_loader.add_value('zipcode',zipcode)
            item_loader.add_value('address',address)
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)
            item_loader.add_value('property_type',property_type)
            item_loader.add_value('room_count',room_count)
            item_loader.add_value('bathroom_count',bathroom_count)
            item_loader.add_value('available_date',available_date)
            item_loader.add_value('images',images)
            item_loader.add_value('external_images_count',external_images_count)
            item_loader.add_value('rent',rent)
            item_loader.add_value('currency','CAD')
            item_loader.add_value('deposit',deposit)
            item_loader.add_value('parking',parking)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('dishwasher',dishwasher)
            item_loader.add_value('landlord_name','propertyhunters')
            item_loader.add_value('landlord_phone','855-373-7368')
            item_loader.add_value('landlord_email','info@propertyhunters.com')
            yield item_loader.load_item()
        else:
            pass
    
