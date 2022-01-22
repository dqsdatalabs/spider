import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json 

class compten_ca_PySpider_canadaSpider(scrapy.Spider):
    name = 'compten_ca'
    allowed_domains = ['compten.ca']
    country = 'canada'
    locale = 'en'
    external_source = "{}_PySpider_{}_{}".format(
        name.capitalize(), country, locale)
    execution_type = 'testing'

    def start_requests(self):
        yield Request(url='https://api.theliftsystem.com/v2/search?show_promotions=true&client_id=44&auth_token=Ns1JeARiadF3jYMYasd1&city_id=3133&geocode=&min_bed=-1&max_bed=3&min_bath=1&max_bath=3&min_rate=0&max_rate=3000&region=&keyword=false&property_types=&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=30&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')
        yield Request(url='https://api.theliftsystem.com/v2/search?show_promotions=true&client_id=44&auth_token=Ns1JeARiadF3jYMYasd1&city_id=1837&geocode=&min_bed=-1&max_bed=3&min_bath=1&max_bath=3&min_rate=0&max_rate=3000&region=&keyword=false&property_types=&amenities=&order=min_rate+ASC%2C+max_rate+ASC%2C+min_bed+ASC%2C+max_bed+ASC&limit=30&offset=0&count=false',
                    callback=self.parse,
                    body='',
                    method='GET')
    

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item['permalink']
            external_id = item['id']
            title = item['name']
            property_type = item['property_type']
            address = item['address']['address']
            city = item['address']['city']
            zipcode = item['address']['postal_code']
            pets_allowed = item['pet_friendly']
            description = item['details']['overview']
            latitude = item['geocode']['latitude']
            longitude = item['geocode']['longitude']
            landlord_name = item['contact']['name']
            landlord_phone = item['contact']['phone']
            landlord_email = item['contact']['email']
            yield Request(url=url, callback=self.parse_property,
            meta={'title':title,
                  'property_type': property_type,
                  'address': address,
                  'city': city,
                  'zipcode': zipcode,
                  'pets_allowed': pets_allowed,
                  'description': description,
                  'latitude':latitude,
                  'longitude':longitude,
                  'landlord_name':landlord_name,
                  'landlord_phone':landlord_phone,
                  'landlord_email':landlord_email})


    def parse_property(self, response):
        number_of_properties = response.css("#content > article > div > div.mainbar > section:nth-child(2) > div > section > table > tbody >tr").extract()
        counter = 1
        for i in range(len(number_of_properties)):
            availability = response.css("div > section > table > tbody > tr:nth-child("+str(counter)+") > td.availability-td.requires-js > a::text").get()
            if availability == 'Inquire Now':  
                item_loader = ListingLoader(response=response)
                floor_plan_images = None
                external_id = response.css("div > section > table > tbody > tr:nth-child("+str(counter)+") > td.availability-td.requires-js > a::attr(href)").get().split("#modal-suite-")[1]
                available_date = "Inquire Now"
                try:
                    floor_plan_images = response.css("div > section > table > tbody > tr:nth-child("+str(counter)+") > td.floorplan-td.requires-js > a::attr(href)").get()
                except:
                    pass                                 
                rent = response.css("div > section > table > tbody > tr:nth-child("+str(counter)+") > td.rent-td > span.price::text").get()
                if "," in rent:
                    rent = rent.replace(",","")
                rent = int(rent.split("$")[1])
                room_count = response.css("div > section > table > tbody > tr:nth-child("+str(counter)+") > td.suite-type-td::text").get()
                if "Bedroom" in room_count:
                    room_count = room_count.split("Bedroom")[0]
                    if "Jr." in room_count:
                        room_count = room_count.split("Jr. ")[1]
                    room_count = int(room_count)
                else:
                    room_count = 1
                bathroom_count = response.css("div > section > table > tbody > tr:nth-child("+str(counter)+") > td.baths-td > span::text").get()
                bathroom_count = int(room_count)
                counter = counter + 1
                
                balcony = None
                elevator = None
                washing_machine = None
                parking = None
                dishwasher = None
                title = response.meta.get("title")
                property_type = response.meta.get("property_type")
                address = response.meta.get("address")
                city = response.meta.get("city")
                zipcode = response.meta.get("zipcode")
                pets_allowed = response.meta.get("pets_allowed")
                description = response.meta.get("description")
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")
                landlord_name = response.meta.get("landlord_name")
                landlord_phone = response.meta.get("landlord_phone")
                landlord_email = response.meta.get("landlord_email")
                

                description = description.replace("\r","").replace("\n","").replace("\t",'')
                if "apartment" in property_type:
                    property_type = 'apartment'
                else:
                    property_type = 'house'
                if pets_allowed == 'n/a':
                    pets_allowed = None
                amenities = response.css("#content > article > div > div.sidebar > section.amenities > div > section > div > ul > li::text").extract()
                images = response.css("#gallery > div.gallery.flexslider > ul.slides > li > a::attr(href)").extract()
                external_images_count = len(images)
                if "Underground parking" in amenities or "Visitor parking" in amenities or "Outdoor parking" in amenities:
                    parking = True
                if "Elevators" in amenities or "elevator" in description:
                    elevator = True
                if "Balconies" in amenities or "balconies" in description:
                    balcony = True
                if "Laundry facilities" in amenities or "laundry" in description:
                    washing_machine = True
                if "dishwasher" in description or "Dishwasher available" in amenities:
                    dishwasher = True
                

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
                item_loader.add_value('floor_plan_images',floor_plan_images)
                item_loader.add_value('external_images_count',external_images_count)
                item_loader.add_value('rent',rent)
                item_loader.add_value('currency','CAD')
                item_loader.add_value('pets_allowed',pets_allowed)
                item_loader.add_value('parking',parking)
                item_loader.add_value('elevator',elevator)
                item_loader.add_value('balcony',balcony)
                item_loader.add_value('washing_machine',washing_machine)
                item_loader.add_value('dishwasher',dishwasher)
                item_loader.add_value('landlord_name',landlord_name)
                item_loader.add_value('landlord_phone',landlord_phone)
                item_loader.add_value('landlord_email',landlord_email)
                yield item_loader.load_item()
            else:
                break