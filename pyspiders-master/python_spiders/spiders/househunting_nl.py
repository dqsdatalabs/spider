# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import math
class MySpider(Spider):
    name = 'househunting_nl'
    execution_type='testing'
    country='netherlands'
    locale='nl'
    external_source= "Househunting_PySpider_netherlands_nl" 

    custom_settings= {
        "HTTPCACHE_ENABLED":False
    }


    headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "https://househunting.nl",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36"
        }

    def start_requests(self):
        start_urls = [
            {"url": "https://househunting.nl/woningaanbod/?available-since=&property-type=appartement&type=for-rent&filter_location=&lat=&lng=&km=&km=&min-price=&max-price=&vestiging=&sort=", "property_type": "apartment", "type":"appartement"},
	        {"url": "https://househunting.nl/woningaanbod/?available-since=&property-type=woonhuis&type=for-rent&filter_location=&lat=&lng=&km=&km=&min-price=&max-price=&vestiging=&sort=", "property_type": "house", "type":"woonhuis"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        page = response.meta.get('page', 2)
        
        seen = False
        if page == 2:
            for item in response.xpath("//ul[@class='locations']/li"):
                follow_url = item.xpath("./a/@href").extract_first()
                city = item.xpath("//div[@class='location_address']/p/text()").extract_first()
                street = item.xpath("//div[@class='location_address']/h3/text()").extract_first()
                yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "city":city, "street":street})
        else:
            data = json.loads(response.body)
            if "posts" in data.keys():
                for item in data["posts"]:
                    follow_url = item["url"]
                    city = item["city"]
                    street = item["title"]
                    yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), "city":city, "street":street})
                    seen = True

        if page == 2 or seen:
            data = {
                "km": "",
                "available-since": "1970-01-01",
                "property-type": response.meta.get("type"),
                "type": "for-rent",
                "t": "1020201",
                "page": f"{page}",
                "sort":"",
            
            }

            yield FormRequest(
                "https://househunting.nl/wp-json/houses/posts",
                formdata=data,
                headers=self.headers,
                callback=self.parse,
                meta={"type":response.meta.get("type"),"page": page+1, 'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        city = (response.url).strip("/").split("-")[-1].strip()
        street = response.xpath("//div[@class='single_adress']/h2/text()").get()
        address = street + ", " + city
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("property_type", response.meta.get("property_type"))
        
        rented = response.xpath("//div[@class='single_media_status']//text()").extract_first()
        if rented and "Verhuurd" in rented:
            return
        title=response.xpath("//title//text()").get()
        if title:
            item_loader.add_value("title",title)
        
        item_loader.add_value("address",address)
        item_loader.add_value("city",city)
        a_date = response.xpath("//ul[@class='details']//span[contains(.,'Beschikbaar per')]/following-sibling::text()").extract_first()
        if a_date:
            item_loader.add_value("available_date",a_date.split(":")[1].strip())

        price = response.xpath("//div[@class='single_price']/h3/text()").extract_first()
        if price:
            item_loader.add_value("rent_string", price)

        square = response.xpath("//ul[@class='details']//span[contains(.,'Oppervlakte')]/following-sibling::text()").extract_first()
        if square:
            square_meters = math.ceil(float(square.split(":")[1].split("m")[0].strip()))
            item_loader.add_value("square_meters",square_meters )
     
        room = response.xpath("//ul[@class='details']//span[contains(.,'Slaapkamers')]/following-sibling::text()").extract_first()
        if room: 
            item_loader.add_value("room_count", room.split(":")[1].strip())



        bathroom_count = response.xpath("//span[contains(.,'Badkamers')]/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(':')[-1].strip())
      
        desc = "".join(response.xpath("//div[@class='single_tab_block']//p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
            if "balkon" in desc:
                item_loader.add_value("balcony", True)
            if "wasmachine" in desc:
                item_loader.add_value("washing_machine", True)
            if "lift" in desc:
                item_loader.add_value("elevator", True) 
            if "garage" in desc:
                item_loader.add_value("parking", True)
            
        deposit = response.xpath("//ul[@class='property-extras']//span[contains(.,'Borg')]/following-sibling::span/text()").extract_first()
        if deposit:
            item_loader.add_value("deposit", deposit.split(" ")[1])

        terrace = response.xpath("//ul[@class='property-extras']//span[contains(.,'terras')]/following-sibling::span/text()").extract_first()
        if terrace:
            if "Nee" in terrace:
                item_loader.add_value("terrace", False)
            else:
                item_loader.add_value("terrace", True)

        furnished = response.xpath("//ul[@class='property-extras']//span[contains(.,'Interieur')]/following-sibling::span/text()").extract_first()
        if furnished:
            if "Gemeubileerd" in furnished:
                item_loader.add_value("furnished", True)
            if "Gestoffeerd" in furnished:
                item_loader.add_value("furnished", False)

        longitude = response.xpath("///div[@class='marker']/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        latitude = response.xpath("//div[@class='marker']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)

        images = [x for x in response.xpath("//div[@class='single_media']//a[contains(@class,'single_gallery_image')]/@href").extract()]
        if images is not None:
            item_loader.add_value("images", images)      

        landlord_email = response.xpath("//a[contains(@href,'mailto')]/text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email",landlord_email)

        # https://househunting.nl/woningaanbod/h105300016-elisabeth-brugsmanweg-den-haag/
        external_id = response.url.strip("/").split("/")[-1].split("-")[0]
        item_loader.add_value("external_id",external_id)

        phone = response.xpath("//a[contains(@href,'tel:')]/text()").get()
        if phone:
            item_loader.add_value("landlord_phone",phone)

        # item_loader.add_xpath("landlord_phone", "//div[@class='single_info_poster_text']//a/text()")
        item_loader.add_value("landlord_name", "House Hunting")
        yield item_loader.load_item()