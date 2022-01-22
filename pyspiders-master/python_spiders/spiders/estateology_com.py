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
import datetime
from datetime import datetime
import dateparser

class MySpider(Spider):
    name = 'estateology_com'     
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://www.estateology.com/properties/?search=rent&search=rent&limitNumber=50&searchterm=London%2C+Birle%C5%9Fik+Krall%C4%B1k&place_id=ChIJdd4hrwug2EcRmSrV3Vo6llI&searchRadius=&propertyType=6&bedrooms=&dateadded=&minmumPrice=&maximumPrice=&locationterm=London&orderBy=DESC",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://www.estateology.com/properties/?search=rent&orderBy=DESC&limitNumber=50&searchterm=London%2C+Birle%C5%9Fik+Krall%C4%B1k&place_id=ChIJdd4hrwug2EcRmSrV3Vo6llI&searchRadius=&propertyType=5&bedrooms=&dateadded=&minmumPrice=&maximumPrice=",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[contains(@class,'cta alt')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//div[contains(@class,'frontPagination')]/a[last()]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Estateology_PySpider_" + self.country + "_" + self.locale)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        zipcode = ""
        city = ""
        title = "".join(response.xpath("//div/h1/text()").extract())
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title))
            item_loader.add_value("address", re.sub("\s{2,}", " ", title))

            city_zipcode = title.split(",")[-1].strip()
            if city_zipcode:
                if " " in city_zipcode:
                    if city_zipcode.count(" ")==1: zipcode = city_zipcode.split(" ")[-1]
                    elif city_zipcode.count(" ") == 2: zipcode = f"{city_zipcode.split(' ')[-2]} {city_zipcode.split(' ')[-1]}"
                    else: zipcode = city_zipcode.split(" ")[-1]
                    print(zipcode)
                    city =  city_zipcode.split(" ")[0]
                else:
                    city =  title.split(",")[-2]
                    if "London" not in city_zipcode:
                        zipcode = city_zipcode
                
                if zipcode:
                    item_loader.add_value("zipcode", zipcode.strip())
                item_loader.add_value("city", city.strip())


        room = "".join(response.xpath("//div[contains(@class,'featured_property_specs')]/i[@class='fa fa-bed']/following-sibling::span[1]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.strip())

        bathroom = "".join(response.xpath("//div[contains(@class,'featured_property_specs')]/img[@alt='bathrooms']/following-sibling::span[1]/text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip())


        price = "".join(response.xpath("substring-after(//span[contains(@class,'single_price')]/text(),'- ')").extract())
        if price:
            item_loader.add_value("rent_string", price.split("PCM")[0].strip().replace(",",".").replace(" ",""))
        # else:
        #     pw_rent = "".join(response.xpath("substring-after(//h2/text()[contains(.,'pw')],'£') ").extract())
        #     if pw_rent:
        #         pw = pw_rent.split("pw")[0]
        #         item_loader.add_value("rent",int(pw)*4 )

        available_date=response.xpath("//span/strong[contains(.,'Date Available:')]/following-sibling::text()").get()

        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)
        else:
            available_date="".join(response.xpath("//ul[contains(@class,'features_list')]/li[contains(.,'Available')]/text()[not(contains(.,'Immediately ') or contains(.,'NOW')) ]").getall())
            if available_date:
                date= "{} {} 11:19:45.301408".format(available_date.split("Available")[0].strip(),datetime.now().year)
                date_parsed = dateparser.parse(date, date_formats=["%d-%m-%Y"])
                date3 = ""
                if date_parsed:
                    date3 = date_parsed.strftime("%Y-%m-%d")

                current_date = str(datetime.now()) 
                if current_date > date3:
                    date = datetime.now().year +1
                    parsed = date3.replace("2020",str(date))
                    item_loader.add_value("available_date", parsed)

        desc = " ".join(response.xpath("//div[@id='tab-2']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        images=[x for x in response.xpath("//div[@class='slide']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))


        floor_plans=[response.urljoin(x) for x in response.xpath("//img[@class='floorplan']/@src").getall()]
        if floor_plans:         
            item_loader.add_value("floor_plan_images", floor_plans)

        Floor = ""
        floor = " ".join(response.xpath("//ul/li[@class='single_feature']/text()[contains(.,'Floor') or contains(.,'floor')]").extract())
        if floor:
            if "Floor" in floor:
                Floor =  floor.split("Floor")[0].strip()
            elif "floor" in floor:
                Floor =  floor.split("floor")[0].replace("-","").strip()

            item_loader.add_value("floor", Floor.strip())

        item_loader.add_xpath("latitude", "substring-before(substring-after(//*[@id='tab-1']/script[2]/text(),'MapLatitude = '),';')")
        item_loader.add_xpath("longitude", "substring-before(substring-after(//*[@id='tab-1']/script[2]/text(),'MapLongitude = '),';')")

        parking = "".join(response.xpath("//div[contains(@class,'featured_property_specs')]/i[@class='fa fa-car']/following-sibling::span[1]/text()").extract())
        if parking:
            item_loader.add_value("parking", True)

        furnished = "".join(response.xpath("//ul/li[@class='single_feature']/text()[contains(.,'Furnished')]").extract())
        if furnished:
            item_loader.add_value("furnished", True)

        balcony = "".join(response.xpath("//ul/li[@class='single_feature']/text()[contains(.,'Balcony')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = "".join(response.xpath("//ul/li[@class='single_feature']/text()[contains(.,'Lift')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = "".join(response.xpath("/li[@class='single_feature']/text()[contains(.,'Terrace')]").extract())
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "EstateOlogy")
        item_loader.add_value("landlord_phone", "020 3422 2333")
        item_loader.add_value("landlord_email", "hello@estateology.com")
        yield item_loader.load_item()
