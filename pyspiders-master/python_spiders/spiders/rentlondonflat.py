# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re
from ..loaders import ListingLoader
from scrapy import Request,FormRequest
from python_spiders.helper import string_found, format_date, remove_white_spaces
import math 
import json 
import dateparser 

class RentlondonflatSpider(scrapy.Spider):
    name = "rentlondonflat"
    allowed_domains = ["rentlondonflat.com"] 

    execution_type = 'testing'
    country = 'united_kingdom' 
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    post_url = "https://www.rentlondonflat.com/wp-admin/admin-ajax.php"
    start_urls = ['https://www.rentlondonflat.com/flats-to-rent-in-london/'] 

    def parse(self,response):
        security_value = response.xpath("//input[@id='securityhomeyMap']/@value").get()
        formdata = {
            "action": "homey_half_map",
            "arrive": "", 
            "depart": "", 
            "guest": "", 
            "keyword": "", 
            "pets": "", 
            "bedrooms": "", 
            "rooms": "", 
            "room_size": "", 
            "search_country": "", 
            "search_city": "", 
            "search_area": "", 
            "listing_type": "", 
            "min-price": "", 
            "max-price": "", 
            "country": "", 
            "state": "", 
            "city": "", 
            "area": "", 
            "booking_type": "", 
            "search_lat": "", 
            "search_lng": "", 
            "radius": "", 
            "start_hour": "", 
            "end_hour": "", 
            "amenity": "", 
            "facility": "", 
            "layout": "list",
            "num_posts": "9",
            "sort_by": "d_date",
            "paged": "0",
            "security": str(security_value),
        }
        yield FormRequest(
            url=self.post_url,
            callback=self.get_parse,
            dont_filter=True,
            formdata=formdata,
            meta={
                "property_type":"apartment",
                "security_value":security_value
            }
        )
        
    def get_parse(self, response):
        page = response.meta.get("page", 2)
        security_value = response.meta.get("security_value")
        data = json.loads(response.body)
        total_item = data["total_results"].split(" ")[0]
        for item in data["listings"]:
            follow_url = item["url"]
            yield Request(follow_url, callback=self.get_property_details, meta={"property_type":response.meta["property_type"],"item":item})
           
        page_num = int(math.ceil(int(total_item) / 9))
        if page <= page_num:
            print("page",page)
            formdata = {
            "action": "homey_half_map",
            "arrive": "", 
            "depart": "", 
            "guest": "", 
            "keyword": "", 
            "pets": "", 
            "bedrooms": "", 
            "rooms": "", 
            "room_size": "", 
            "search_country": "", 
            "search_city": "", 
            "search_area": "", 
            "listing_type": "", 
            "min-price": "", 
            "max-price": "", 
            "country": "", 
            "state": "", 
            "city": "", 
            "area": "", 
            "booking_type": "", 
            "search_lat": "", 
            "search_lng": "", 
            "radius": "", 
            "start_hour": "", 
            "end_hour": "", 
            "amenity": "", 
            "facility": "", 
            "layout": "list",
            "num_posts": "9",
            "sort_by": "d_date",
            "paged": str(page),
            "security": str(security_value),
            }
            yield FormRequest(
                url=self.post_url,
                callback=self.get_parse,
                dont_filter=True,
                formdata=formdata,
                meta={
                    "property_type":"apartment","security_value":security_value,"page":page+1
                }
            )
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))


        item = response.meta.get("item")
        external_id = str(item["id"])
        title = item["title"]
        lat = item["lat"]
        lon = item["long"]
        address = item["address"]
        # room_count = item["beds"]
        bathroom_count = item["baths"]
        city = ""
        zipcode = ""
        if address:
            zipcode = address.split(',')[-1].split("(")[0]
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_value('zipcode', zipcode)
        city = response.xpath('//ol[@class="breadcrumb"]//li[last()]/text()').extract_first()
        if city:
            item_loader.add_value("city", city.split("- ")[0])

        external_link = response.url 
        item_loader.add_value('external_link', external_link)

        property_type = response.meta.get('property_type')
        if property_type:
            item_loader.add_value('property_type', property_type)

        rent_month = response.xpath('//li[contains(.,"Rent Per Month:")]/strong/text()').extract_first()
        if rent_month:
            item_loader.add_value('rent_string', rent_month.split(".")[0])  
        renta=item_loader.get_output_value("rent") 
        if not renta: 
            rent_month1=response.xpath('//li[contains(.,"Per Week:")]/strong/text()').extract_first()
            if rent_month1:
                rent_month1=rent_month1.split(".")[0]
                rent=re.findall("\d+",rent_month1)
                if len(rent)==2:
                   rent=int(rent[0]+rent[1])
                   item_loader.add_value('rent', rent*4) 
                else:
                   item_loader.add_value('rent', int(rent[0])*4) 
        item_loader.add_value("currency", "GBP")             
          

        deposit = response.xpath('//li[contains(.,"Deposit")]/strong/text()').extract_first('')
        if deposit:
            item_loader.add_value('deposit', deposit)

        available_date = response.xpath('//li[contains(.,"Next Available:")]/strong/text()').extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        square_meters = response.xpath('//li[contains(.,"Est. Size:")]/strong/text()').extract_first()
        if square_meters and 'sqft' in square_meters.lower():
                square_meters_text_1 = re.findall(r'\d+', square_meters.replace("~",""))[0]
                square_meters = int(int(square_meters_text_1) / 10.764)

        energy_label = response.xpath('//li[contains(.,"EPC Rating")]/strong/text()').extract_first()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)

        pets = response.xpath('//li[contains(.,"EPC Rating")]/strong/text()').extract_first()
        if pets:
            if "No" in pets:
                item_loader.add_value("pets_allowed", False)
            else:
                item_loader.add_value("pets_allowed", True)

        external_id = response.xpath('//li[contains(.,"Property Reference")]/strong/text()').extract_first()
        if external_id:
            item_loader.add_value("external_id", external_id)

        item_loader.add_xpath('description', '//div[@id="about-section"]//div[@class="block-body"]//p//text()')
    
        item_loader.add_value('bathroom_count', bathroom_count)
        room_count=response.xpath("//strong[contains(.,'Beds')]/text()").get()
        if room_count:
            room_count=room_count.split("/")[0]
            room_count=re.findall("\d+",room_count)
        item_loader.add_value('room_count', room_count)
        
        images = [x for x in response.xpath("//div[@class='listing-slider-variable-width']//img/@src[not(contains(.,'floorplan'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        floor_plan_images = [x for x in response.xpath("//div[@class='listing-slider-variable-width']//img/@src[contains(.,'floorplan')]").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
    
        item_loader.add_value('latitude', str(lat))
        item_loader.add_value('longitude', str(lon))
        item_loader.add_value('landlord_name', 'London Rent Flat')
        item_loader.add_value('landlord_email', 'enquiries@rentlondonflat.com')
        item_loader.add_value('landlord_phone', '+44 (0) 207 993 1398')
        yield item_loader.load_item()