import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json

class ranchowinnipeg_com_PySpider_canadaSpider(scrapy.Spider):
    name = 'ranchowinnipeg_com'
    allowed_domains = ['ranchowinnipeg_com','residential.ranchowinnipeg.com']
    start_urls = [
        'https://residential.ranchowinnipeg.com/apartments'
        ]
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?locale=en&client_id=200&auth_token=sswpREkUtyeYjeoahA2i&city_id=3377&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=0&max_rate=2300&min_sqft=0&max_sqft=10000&show_custom_fields=true&show_promotions=true&region=&keyword=false&property_types=apartments%2C+houses%2C+commercial&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=min_rate+ASC&limit=66&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')
    
    def parse(self, response):  
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            yield Request(url=item['permalink'],callback=self.parse_property,
            meta={
                "external_id":str(item['id']),
                "title":item['website']['title'],
                "property_type":item['property_type'],
                "landlord_name":item['client']['name'],
                "landlord_email":item['client']['email'],
                "landlord_phone":item['client']['phone'],
                "address":item['address']['address'],
                "city":item['address']['city'],
                "zipcode":item['address']['postal_code'],
                "latitude":item['geocode']['latitude'],
                "longitude":item['geocode']['longitude'],
                "pets_allowed":item['pet_friendly'],
                "description":item['details']['overview']
            })


    def parse_property(self, response):
        
        suite_info = response.css(".suite-wrap").extract()
        counter = 2
        for i in range(len(suite_info)):
            item_loader = ListingLoader(response=response)
            room_count = response.css("#suites > section > div > div:nth-child("+str(counter)+") > div > div.suite-type.cell::text").get()
            if "Bachelor" in room_count:
                room_count = 1
            elif "Bedroom" in room_count:
                room_count = int(room_count.split("Bedroom")[0])
            bathroom_count = response.css("#suites > section > div > div:nth-child("+str(counter)+") > div > div.suite-bath.cell > span.value::text").get().strip()
            if ".5" in bathroom_count:
                bathroom_count = int(bathroom_count.replace(".5",""))
            rent = int(response.css("#suites > section > div > div:nth-child("+str(counter)+") > div > div.suite-rate.cell::text").get().strip().replace("$",""))
            available_date = response.css("#suites > section > div > div:nth-child("+str(counter)+") > div > div.suite-availability.cell > a::text").get()
            counter = counter + 1

            title = response.meta.get("title")
            description= response.meta.get("description")
            property_type = str(response.meta.get("property_type"))
            if 'house' in property_type:
                property_type = 'house'
            else:
                property_type = 'apartment'
            external_id = response.meta.get("external_id")
            landlord_name = response.meta.get("landlord_name")
            landlord_email = response.meta.get("landlord_email")
            landlord_phone = response.meta.get("landlord_phone")
            address = response.meta.get("address")
            city = response.meta.get("city")
            zipcode = response.meta.get("zipcode")
            latitude = response.meta.get("latitude")
            longitude = response.meta.get("longitude")
            pets_allowed = response.meta.get("pets_allowed")

            amenities = response.css(".amenity-holder::text").extract()
            for i in range(len(amenities)):
                amenities[i] = amenities[i].strip()
            parking = None
            elevator = None
            balcony = None
            terrace = None
            swimming_pool = None
            washing_machine = None
            dishwasher = None
            if "Covered parking" in amenities or "Outdoor parking" in amenities or "Visitor parking" in amenities or "parking" in description:
                parking = True
            if 'Elevators' in amenities:
                elevator = True
            if "Balconies" in amenities or "balcony" in description or "balconies" in description: 
                balcony = True
            if "terrace" in description or "patios" in description or "Patios" in amenities:
                terrace = True
            if "Outdoor pool" in amenities:
                swimming_pool = True
            if "Washer in suite" in amenities or "Laundry facilities" in amenities:
                washing_machine = True
            if "Dishwasher available" in amenities:
                dishwasher = True

            images = response.css(".cover").extract()
            for i in range(len(images)):
                images[i]=images[i].split('data-src2x="')[1].split('" class=')[0]
            external_images_count = len(images)
            

            item_loader.add_value('external_link', response.url)
            item_loader.add_value('external_id',external_id)        
            item_loader.add_value('external_source', self.external_source)
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
            item_loader.add_value('pets_allowed',pets_allowed)
            item_loader.add_value('parking',parking)
            item_loader.add_value('elevator',elevator)
            item_loader.add_value('balcony',balcony)
            item_loader.add_value('terrace',terrace)
            item_loader.add_value('swimming_pool',swimming_pool)
            item_loader.add_value('washing_machine',washing_machine)
            item_loader.add_value('dishwasher',dishwasher)
            item_loader.add_value('landlord_name',landlord_name)
            item_loader.add_value('landlord_email',landlord_email)
            item_loader.add_value('landlord_phone',landlord_phone)
            yield item_loader.load_item()