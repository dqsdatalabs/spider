import scrapy
from scrapy import Request
from ..loaders import ListingLoader
import json
import math

class bluestoneproperties_com_PySpider_canadaSpider(scrapy.Spider):
    name = "bluestoneproperties_com"
    allowed_domains = ["bluestoneproperties.com"]
    start_urls = ["https://www.bluestoneproperties.com/residential/cities/london"]
    country = "canada"
    locale = "en"
    external_source = "{}_PySpider_{}_{}".format(name.capitalize(), country, locale)
    execution_type = "testing"

    def start_requests(self):
        yield Request(
            url="https://api.theliftsystem.com/v2/search?client_id=449&auth_token=sswpREkUtyeYjeoahA2i&city_id=1607&geocode=&min_bed=-1&max_bed=100&min_bath=0&max_bath=10&min_rate=1100&max_rate=2400&min_sqft=0&max_sqft=10000&show_promotions=true&local_url_only=true&region=&keyword=false&property_types=apartments%2C+houses&ownership_types=&exclude_ownership_types=&custom_field_key=&custom_field_values=&order=max_rate+ASC%2C+min_rate+ASC%2C+min_bed+ASC%2C+max_bath+ASC&limit=50&neighbourhood=&amenities=&promotions=&city_ids=&pet_friendly=&offset=0&count=false",
            callback=self.parse,
            body="",
            method="GET",
        )

    def parse(self, response):
        parsed_response = json.loads(response.body)
        for item in parsed_response:
            url = item["permalink"]
            external_id = item["id"]
            title = item["name"]
            property_type = item["property_type"]
            address = item["address"]["address"]
            city = item["address"]["city"]
            zipcode = item["address"]["postal_code"]
            pets_allowed = item["pet_friendly"]
            latitude = item["geocode"]["latitude"]
            longitude = item["geocode"]["longitude"]
            landlord_name = item["client"]["name"]
            landlord_phone = item["client"]["phone"]
            landlord_email = item["client"]["email"]
            yield Request(
                url=url,
                callback=self.parse_property,
                meta={
                    "title": title,
                    "external_id": str(external_id),
                    "property_type": property_type,
                    "address": address,
                    "city": city,
                    "zipcode": zipcode,
                    "pets_allowed": pets_allowed,
                    "latitude": latitude,
                    "longitude": longitude,
                    "landlord_name": landlord_name,
                    "landlord_phone": landlord_phone,
                    "landlord_email": landlord_email,
                },
            )

    def parse_property(self, response):

        rooms_info = response.css("section.widget.suites > div.suites > div > div > div.table").extract()
        if rooms_info is not None:
            if len(rooms_info) > 1:
                counter = 1 
                for i in range(len(rooms_info)):
                    item_loader = ListingLoader(response=response)

                    room_count = response.css("section.widget.suites > div.suites > div:nth-child("+str(counter)+") > div > div.table > div.suite-type.cell > div > div > a:nth-child("+str(counter)+")::text").get()
                    rent = int(response.css("section.widget.suites > div.suites > div:nth-child("+str(counter)+") > div > div.table > div.suite-rate.cell > div::text").get().replace("$",""))
                    square_meters = math.ceil(int(response.css("section.widget.suites > div.suites > div:nth-child("+str(counter)+") > div > div.table > div.suite-sqft.cell > p::text").get().replace("sq.ft.",""))/10.764)
                    available_date = response.css("section.widget.suites > div.suites > div:nth-child("+str(counter)+") > div > div.table > div.suite-availability.cell > p::text").get().strip()
                    floor_plan_images = response.css("section.widget.suites > div.suites > div:nth-child("+str(counter)+") > div > div.table > div.suite-floorplans.cell > a::attr(href)").get()           
                    if "1" in room_count or "One" in room_count:
                        room_count = 1
                    elif "2" in room_count or "Two" in room_count:
                        room_count = 2
                    else:
                        room_count = 3
                    counter = counter+1

                    description = None
                    try:
                        description = response.css(
                            "#content > div:nth-child(4) > div > div > div.property-details > div > div.page-content.cms-content > p:nth-child(2)::text").get()
                    except:
                        pass
                    if description is None:
                        description = response.css("div.page-content.cms-content > div::text").get()
                    if description is None:
                        description = response.css("#content > div:nth-child(4) > div > div > div > div > div.page-content.cms-content > p:nth-child(3)::text").get()
                    if description is None:
                        description = response.css("#content > div:nth-child(4) > div > div > div > div > div.page-content.cms-content > p:nth-child(2) > span::text").get()
                
                    amenities = response.css("div.amenities-container > div > div > ul > li > span::text").extract()
                    parking = None
                    elevator = None
                    balcony = None
                    washing_machine = None
                    dishwasher = None
                    body = response.css("body").get()
                    if "Outdoor parking" in body or "Free Parking" in body or "Underground parking" in body:
                        parking = True
                    if "Elevators" in body:
                        elevator = True
                    if "Balconies" in body or "balconies" in body:
                        balcony = True
                    if "Laundry facilities" in body:
                        washing_machine = True
                    if  "Dishwasher" in body:
                        dishwasher = True
                    images = response.css("#slickslider-default-id-0 .cover").extract()
                    for i in range(len(images)):
                        images[i] = images[i].split('data-src2x="')[1].split('"')[0]
                    external_images_count = len(images)


                    title = response.meta.get("title")
                    external_id = response.meta.get("external_id")
                    property_type = response.meta.get("property_type")
                    address = response.meta.get("address")
                    city = response.meta.get("city")
                    zipcode = response.meta.get("zipcode")
                    pets_allowed = response.meta.get("pets_allowed")
                    latitude = response.meta.get("latitude")
                    longitude = response.meta.get("longitude")
                    landlord_name = response.meta.get("landlord_name")
                    landlord_phone = response.meta.get("landlord_phone")
                    landlord_email = response.meta.get("landlord_email")

                    if pets_allowed is not True and pets_allowed is not False:
                        pets_allowed = None
                    if "apartment" in property_type:
                        property_type = 'apartment'
                    else:
                        property_type = 'house'


                    item_loader.add_value("external_link", response.url)
                    item_loader.add_value("external_id", external_id)
                    item_loader.add_value("external_source", self.external_source)
                    item_loader.add_value("title", title)
                    item_loader.add_value('description',description)
                    item_loader.add_value("city", city)
                    item_loader.add_value("zipcode", zipcode)
                    item_loader.add_value("address", address)
                    item_loader.add_value("latitude", latitude)
                    item_loader.add_value("longitude", longitude)
                    item_loader.add_value("property_type", property_type)
                    item_loader.add_value('square_meters',int(int(square_meters)*10.764))
                    item_loader.add_value('room_count',room_count)
                    item_loader.add_value('available_date',available_date)
                    item_loader.add_value('images',images)
                    item_loader.add_value('floor_plan_images',floor_plan_images)
                    item_loader.add_value('external_images_count',external_images_count)
                    item_loader.add_value('rent',rent)
                    item_loader.add_value('currency',"CAD")
                    item_loader.add_value("pets_allowed", pets_allowed)
                    item_loader.add_value('parking',parking)
                    item_loader.add_value('elevator',elevator)
                    item_loader.add_value('balcony',balcony)
                    item_loader.add_value('washing_machine',washing_machine)
                    item_loader.add_value('dishwasher',dishwasher)
                    item_loader.add_value("landlord_name", landlord_name)
                    item_loader.add_value("landlord_phone", landlord_phone)
                    item_loader.add_value("landlord_email", landlord_email)
                    yield item_loader.load_item()
                
            elif len(rooms_info) == 1:
                item_loader = ListingLoader(response=response)
                try:
                    room_count = response.css("section.widget.suites > div.suites > div > div > div.table > div.suite-type.cell > div > div > a:nth-child(1)::text").get().split("Bedroom")[0]
                except:
                    room_count = response.css("section.widget.suites > div.suites > div > div > div.table > div.suite-type.cell > div::text").get()                
                room_count = room_count.split("Bedroom")[0]
                if "1" in room_count or "one" in room_count or 'Bachelor' in room_count:
                    room_count = 1
                elif "2" in room_count or "Two" in room_count:
                    room_count = 2
                rent = int(response.css("section.widget.suites > div.suites > div > div > div.table > div.suite-rate.cell > div::text").get().replace("$",""))
                square_meters = math.ceil(int(response.css("section.widget.suites > div.suites > div > div > div.table > div.suite-sqft.cell > p::text").get().replace("sq.ft.",""))/10.764)
                available_date = response.css("section.widget.suites > div.suites > div > div > div.table > div.suite-availability.cell > p::text").get().strip()
                floor_plan_images = response.css("section.widget.suites > div.suites > div > div > div.table > div.suite-floorplans.cell > a::attr(href)").extract()           
                
                description = None
                try:
                    description = response.css(
                        "#content > div:nth-child(4) > div > div > div.property-details > div > div.page-content.cms-content > p:nth-child(2)::text").get()
                except:
                    pass
                if description is None:
                    description = response.css("div.page-content.cms-content > div::text").get()
                if description is None:
                    description = response.css("#content > div:nth-child(4) > div > div > div > div > div.page-content.cms-content > p:nth-child(3)::text").get()
                if description is None:
                    description = response.css("#content > div:nth-child(4) > div > div > div > div > div.page-content.cms-content > p:nth-child(2) > span::text").get()
            
                amenities = response.css("div.amenities-container > div > div > ul > li > span::text").extract()
                parking = None
                elevator = None
                balcony = None
                washing_machine = None
                dishwasher = None
                body = response.css("body").get()
                if "Outdoor parking" in body or "Free Parking" in body or "Underground parking" in body:
                    parking = True
                if "Elevators" in body:
                    elevator = True
                if "Balconies" in body or "balconies" in body:
                    balcony = True
                if "Laundry facilities" in body:
                    washing_machine = True
                if  "Dishwasher" in body:
                    dishwasher = True
                images = response.css("#slickslider-default-id-0 .cover").extract()
                for i in range(len(images)):
                    images[i] = images[i].split('data-src2x="')[1].split('"')[0]
                external_images_count = len(images)


                title = response.meta.get("title")
                external_id = response.meta.get("external_id")
                property_type = response.meta.get("property_type")
                address = response.meta.get("address")
                city = response.meta.get("city")
                zipcode = response.meta.get("zipcode")
                pets_allowed = response.meta.get("pets_allowed")
                latitude = response.meta.get("latitude")
                longitude = response.meta.get("longitude")
                landlord_name = response.meta.get("landlord_name")
                landlord_phone = response.meta.get("landlord_phone")
                landlord_email = response.meta.get("landlord_email")

                if pets_allowed is not True and pets_allowed is not False:
                    pets_allowed = None
                if "apartment" in property_type:
                    property_type = 'apartment'
                else:
                    property_type = 'house'


                item_loader.add_value("external_link", response.url)
                item_loader.add_value("external_id", external_id)
                item_loader.add_value("external_source", self.external_source)
                item_loader.add_value("title", title)
                item_loader.add_value('description',description)
                item_loader.add_value("city", city)
                item_loader.add_value("zipcode", zipcode)
                item_loader.add_value("address", address)
                item_loader.add_value("latitude", latitude)
                item_loader.add_value("longitude", longitude)
                item_loader.add_value("property_type", property_type)
                item_loader.add_value('square_meters',int(int(square_meters)*10.764))
                item_loader.add_value('room_count',room_count)
                item_loader.add_value('available_date',available_date)
                item_loader.add_value('images',images)
                item_loader.add_value('floor_plan_images',floor_plan_images)
                item_loader.add_value('external_images_count',external_images_count)
                item_loader.add_value('rent',rent)
                item_loader.add_value('currency',"CAD")
                item_loader.add_value("pets_allowed", pets_allowed)
                item_loader.add_value('parking',parking)
                item_loader.add_value('elevator',elevator)
                item_loader.add_value('balcony',balcony)
                item_loader.add_value('washing_machine',washing_machine)
                item_loader.add_value('dishwasher',dishwasher)
                item_loader.add_value("landlord_name", landlord_name)
                item_loader.add_value("landlord_phone", landlord_phone)
                item_loader.add_value("landlord_email", landlord_email)
                yield item_loader.load_item()
            
                
