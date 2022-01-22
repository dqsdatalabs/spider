# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from datetime import datetime
from python_spiders.helper import ItemClear
import re

class MySpider(Spider):
    name = 'stafford_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source="Stafford_Ljhooker_Com_PySpider_australia"
    # custom_settings = {
    #     "PROXY_ON":"True",
    # }
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://stafford.ljhooker.com.au/search/unit_apartment+terrace-for-rent/page-{}?surrounding=True&liveability=False",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://stafford.ljhooker.com.au/search/house+townhouse+duplex_semi_detached+penthouse-for-rent/page-{}?surrounding=True&liveability=False",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://stafford.ljhooker.com.au/search/studio-for-rent/page-{}?surrounding=True&liveability=False",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(1),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[@class='property-details']/h3/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base = response.meta["base"]
            p_url = base.format(page)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", self.external_source)    

        external_id = response.xpath("//div[contains(@class,'code')]//text()").get()
        if external_id:
            external_id = external_id.split("ID")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        title = " ".join(response.xpath("//h2[not(contains(.,'$'))]//text()").getall())
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        desc = " ".join(response.xpath("//div[contains(@class,'property-text')]//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//span[contains(@class,'bed')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//span[contains(@class,'bath')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.xpath("//span[contains(@class,'car')]//text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        address = response.xpath("//h1//text()").get()
        if address:
            item_loader.add_value("address", address)
            if "Street" in address:
                if "Everton Park" in address:
                    city = address.split("Street")[0].strip().split(" ")[-1]
                else:
                    city = address.split("Street")[1].split(",")[0].strip()
            elif "Road" in address:
                if "Everton Park" in address:
                    city = address.split("Road")[0].strip().split(" ")[-1]
                else:
                    city = address.split("Road")[1].split(",")[0]
            else:
                if "Alva" in address:
                    city = "Alva"
                else:
                    city = address.split(",")[0].strip().split(" ")[-1]
            item_loader.add_value("city", city.strip())

        rent = "".join(response.xpath("//h2//text()").getall())
        if rent and not "Application" in rent:
            rent = rent.split("$")[1].strip().split(" ")[0].replace("/","").replace("Ground","").strip()
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")

        square_meters = response.xpath("//strong[contains(.,'Land Area')]//parent::li/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)

        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]//text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        swimming_pool = response.xpath("//h3//following-sibling::div//li[contains(.,'Pool')]//text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)
        
        pets_allowed = response.xpath("//strong[contains(.,'Pets Allowed')]//parent::li[contains(.,'Yes')]/text()").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", True)

        from datetime import datetime
        import dateparser
        available_date = response.xpath("//strong[contains(.,'Available')]//parent::li/text()").get()
        if available_date:
            available_date = available_date.strip()
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
    
        images = [x for x in response.xpath("//div[contains(@id,'slideshow')]//@data-cycle-src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//meta[contains(@property,'og:latitude')]//@content").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        
        longitude = response.xpath("//meta[contains(@property,'og:longitude')]//@content").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        zipcode = response.xpath("//script[contains(.,'postcode')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split('"postcode": "')[-1].split('"')[0].strip())

        item_loader.add_value("landlord_name", "LJ Hooker Stafford")
        item_loader.add_value("landlord_phone", "07 3357 2999")
        item_loader.add_value("landlord_email", "rentals.stafford@ljhooker.com.au")

        yield item_loader.load_item()
