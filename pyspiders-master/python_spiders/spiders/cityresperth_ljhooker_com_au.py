# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re  
import dateparser
from datetime import datetime

class MySpider(Spider):
    name = 'cityresperth_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source='Cityresperth_Ljhooker_Com_PySpider_australia'
    
    def start_requests(self): 

        start_urls = [
            {
                "url" : [
                    "https://cityresperth.ljhooker.com.au/search/unit_apartment-for-rent/page-1?surrounding=true",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://cityresperth.ljhooker.com.au/search/house+townhouse+duplex_semi_detached+penthouse+terrace-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://cityresperth.ljhooker.com.au/search/studio-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-content')]/div[@onclick]//a[contains(@class,'property-sticker')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//div[@class='blog-post']/strong[contains(.,'Sorry')]//text()").get()
        if status:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        night = response.xpath("//div[@class='property-heading']/h2/text()").extract_first()
        if night and "night" in night.lower():
            return
        item_loader.add_value("external_source", "Cityresperth_Ljhooker_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("//div[@class='property-heading']//h2/text()").extract())
        if rent and "$" in rent:
            if rent and 'week' in rent.lower():
                if rent and '.' in rent.lower():
                    rent = rent.split('$')[-1].split('.')[0]
                    price = (int(rent)*4)
                    item_loader.add_value('rent', price)
            else:
                rent = rent.split('$')[-1].split(' ')[0]
                item_loader.add_value('rent', rent)
        else:
            rent = "".join(response.xpath("//meta[contains(@name,'price:amount')]//@content").extract())
            if rent and "$" in rent:     
                if rent and 'week' in rent.lower():
                    if rent and '.' in rent.lower():
                        rent = rent.split('$')[-1].split('.')[0]
                        price = (int(rent)*4)
                        item_loader.add_value('rent', price)
                    else:
                        rent = rent.split('$')[-1].split(' ')[0]
                        price = (int(rent)*4)
                        item_loader.add_value('rent', price)
                else:
                    rent = rent.split('$')[-1].split(' ')[0]
                    item_loader.add_value('rent', rent)

        rentcheck=item_loader.get_output_value("rent")
        if not rentcheck:
            rent="".join(response.xpath("//div[@class='property-heading']//h2/text()").extract())
            if rent:
                rent=rent.split('$')[-1].split('Per')[0]
                item_loader.add_value("rent",(int(rent)*4))

        item_loader.add_value("currency","AUD")

        room_count = response.xpath("//script[contains(.,'bedroom')]//text()").get()
        if room_count:
            room_count = room_count.split('bedrooms": "')[1].split('"')[0]
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//script[contains(.,'bathroom')]//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.split('bathrooms": "')[1].split('"')[0]
            item_loader.add_value("bathroom_count", bathroom_count)

        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address)
        else:
            address = "".join(response.xpath("//title/text()").get())
            if address:
                address = address.split("-")[0].strip()
                item_loader.add_value("address", address)
            
        city =  response.xpath("//meta[contains(@property,'locality')]/@content").get()
        if city:
            item_loader.add_value("city",city)
        else:
            city = response.xpath("//script[contains(.,'postcode')]/text()").re_first(r'"suburb": "(\w+\s*\w*)"')
            if city:
                item_loader.add_value("city",city)
        zipcode = response.xpath("//meta[contains(@property,'postal-code')]/@content").get()
        if zipcode:
            item_loader.add_value("zipcode",zipcode)
        else:
            zipcode = response.xpath("//script[contains(.,'postcode')]/text()").re_first(r'"postcode": "(\d{4})"')
            if zipcode:
                item_loader.add_value("zipcode",zipcode)
        item_loader.add_xpath("latitude","//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude","//meta[@property='og:longitude']/@content")

        desc =  " ".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        else:
            desc =  " ".join(response.xpath("//div[contains(@class,'detail-text')]//text()").extract())
            if desc:
                item_loader.add_value("description", desc.strip())

        external_id =  " ".join(response.xpath("//div[@class='code']/text()").extract())
        if external_id:
            ext_id = external_id.split("ID")[1].strip()
            item_loader.add_value("external_id", ext_id)

        images = [ x for x in response.xpath("//div[@class='thumb']/span/img/@data-cycle-src").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date="".join(response.xpath("//ul/li/strong[.='Date Available:']/following-sibling::text()").getall())
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)


        parking = "".join(response.xpath("//script[contains(.,'parking')]//text()").extract())
        if parking:
            parking = parking.split('parking": "')[1].split('"')[0]
            if parking!="":
                item_loader.add_value("parking", True)

        dishwasher = "".join(response.xpath("//div[@class='col-md-7']/ul/li[contains(.,'Dishwasher')]/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        furnished = "".join(response.xpath("//div[@class='col-md-7']/ul/li[contains(.,'Furnished') or contains(.,'furnished')]/text()").extract())
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        elevator = "".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()[contains(.,'Elevator')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = "".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()[contains(.,'balcony ')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)
        
        yield Request(f"{response.url}/Home/GetProperty", self.parse_landlord, meta={"item_loader": item_loader})
        
    def parse_landlord(self, response):
        item_loader = response.meta.get('item_loader')
        try:
            data = json.loads(response.body)
            desc = data["Advertisement"]
            if desc:
                item_loader.add_value("description", desc)
            latitude = data["LocalityLatitude"]
            if latitude:
                item_loader.add_value("latitude", str(latitude))
            longitude = data["LocalityLongitude"]
            if longitude:
                item_loader.add_value("longitude", str(longitude))
            deposit = data["Bond"]
            if deposit:
                item_loader.add_value("deposit", int(float(deposit)))
            images = data["Images"]
            if images:
                for image in images:
                    item_loader.add_value("images", image["Raw"])

            parking = data["Parking"]
            if parking:
                item_loader.add_value("parking", True)

            features = data["Includes"]
            if features:
                for item in features:
                    if item.lower() in "furnished":
                        item_loader.add_value("furnished", True)
                    if item.lower() in "dishwasher":
                        item_loader.add_value("dishwasher", True)
                    if item.lower() in "pool":
                        item_loader.add_value("swimming_pool", True)
            available_date = data["AdditionalInfo"]
            if available_date:
                for item in available_date:
                    if item["Title"] == "Date Available":
                        available_date = item["Value"]
                        if available_date:
                            if not "now" in available_date.lower():
                                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                                if date_parsed:
                                    date2 = date_parsed.strftime("%Y-%m-%d")
                                    item_loader.add_value("available_date", date2)
            latitude_longitude = data["Agents"]
            item_loader.add_value("landlord_name", f"{latitude_longitude[0]['Salutation']} {latitude_longitude[0]['Surname']}")
            item_loader.add_value("landlord_phone", latitude_longitude[0]['Mobile'])
            item_loader.add_value("landlord_email", latitude_longitude[0]['Email'])
        except:
            item_loader.add_value("landlord_name", "LJ Hooker City Residential")
            item_loader.add_value("landlord_phone", "(08) 9325 0700")
            item_loader.add_value("landlord_email", "cityresperth@ljhooker.com.au")
        yield item_loader.load_item()