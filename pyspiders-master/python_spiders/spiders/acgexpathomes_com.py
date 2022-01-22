# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from html.parser import HTMLParser
import re

class MySpider(Spider):
    name = 'acgexpathomes_com'    

    execution_type='testing'
    country='turkey'
    locale='tr'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://acgexpathomes.com/property-search/?status=for-rent&type=apartment",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://acgexpathomes.com/property-search/?status=for-rent&type=villa",
                    "https://acgexpathomes.com/property-search/?status=for-rent&type=single-family-home",
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

        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@class='more-details']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'),'address': response.meta.get('address')})
            seen = True
        

        if page == 2 or seen:
            if response.meta.get("property_type") == "apartment":
                url = f"https://acgexpathomes.com/property-search/page/{page}/?status=for-rent&type=apartment"
                yield Request(
                    url=url,
                    callback=self.parse,
                    meta={'property_type': response.meta.get('property_type')}
                )
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", "Acgexpathomes_com_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath("title", "//h1[@class='page-title']//text()")

        item_loader.add_value("city",  "Ankara")

        price = "".join(response.xpath("normalize-space(//h5[@class='price']/span[not(@class)]/text())").extract())
        if price:
            if "euros" in price.lower():
                price += "€"
            elif "usd" in price.lower():
                price += "$"
            elif "tl" in price.lower():
                price += "₺"
            item_loader.add_value("rent_string", price.replace(".","").replace(",",""))

        # address = "".join(response.xpath("//dl/dt[. ='Adres']/following-sibling::dd/text()").extract())
        # if address:
        #     item_loader.add_value("address", re.sub("\s{2,}", " ", address))

        meters = "".join(response.xpath("normalize-space(//div[@class='property-meta clearfix']/span[contains(.,'Sq') or contains(.,'sq')]/text())").extract())
        if meters:
            item_loader.add_value("square_meters", meters.split(" ")[0].strip())

        room = "".join(response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Bedroom')]/text()").extract())
        if room:
            item_loader.add_value("room_count", room.replace("Bedrooms", "").strip())
        
        bathroom = "".join(response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Bathroom')]/text()").extract())
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom.strip().split("\xa0")[0])

        # utilities = "".join(response.xpath("//div[contains(@class,'content')]/p[contains(.,'MAINTENANCE')]/text()").extract())
        # if utilities:
        #     item_loader.add_value("utilities", utilities.split(":")[1].strip().split("TL")[0])

        desc = " ".join(response.xpath("//div[contains(@class,'content')]/*[self::p or self::ul]//text()").extract())
        item_loader.add_value("description", desc.strip())

        if not item_loader.get_collected_values("rent_string"):
            if "price" in desc.lower() and "$" in desc:
                item_loader.add_value("rent_string", desc.lower().split("price:")[1].split("$")[0] + "$")
            elif "price" in desc.lower() and "€" in desc:
                item_loader.add_value("rent_string", desc.lower().split("price:")[1].split("€")[0] + "€")
        
        # if "deposit" in desc.lower() and "month rental" not in desc.lower():
        #     item_loader.add_value("deposit", desc.lower().split("deposit:")[1].split("$")[0])
                

        images = [response.urljoin(x)for x in response.xpath("//a[@class='swipebox']/img/@src").extract()]
        if images:
                item_loader.add_value("images", images)

        external_id = "".join(response.xpath("//h4[@class='title']/text()[not(contains(.,'Features'))]").extract())
        if external_id:
            item_loader.add_value("external_id", external_id.split(":")[1].strip())

        parking = response.xpath("//div[@class='property-meta clearfix']/span[contains(.,'Garage')]/text()[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("parking", True)
       
        # terrace = response.xpath("//ul/li/a[contains(.,'Terrace')]/text()").get()
        # if terrace:
        #     item_loader.add_value("terrace", True)

        furnished = response.xpath("//h1/span[contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        features = [x.lower() for x in response.xpath("//div[@class='features']/ul/li/a/text()").getall()]
        if features:
            if "terrace" in features:
                item_loader.add_value("terrace", True)
            if "furnished" in features and "unfurnished" not in features:
                item_loader.add_value("furnished", True)
            if "parking" in features:
                item_loader.add_value("parking", True)
            if "elevator" in features:
                item_loader.add_value("elevator", True)
        
        map_script = response.xpath("//script[contains(.,'initialize_property_map')]/text()").get()
        if map_script:
            data = json.loads(map_script.split("propertyMarkerInfo = ")[1].split("}")[0].strip() + "}")
            if data and "lat" in data and "lang" in data:
                item_loader.add_value("latitude", data["lat"])
                item_loader.add_value("longitude", data["lang"])
            if data and "title" in data:
                if "Sale" in data["title"]: return
                if " in " in data["title"] or " at " in data["title"]: item_loader.add_value("address", data["title"].replace(" at "," in ").split(" in ")[-1].strip())
                elif " near " in data["title"]: item_loader.add_value("address", data["title"].split(",")[0].strip())
                elif "Floor" in data["title"]: item_loader.add_value("address", data["title"].split("Floor")[-1].strip().strip(",").strip())
                elif "Furnished" in data["title"]:item_loader.add_value("address", data["title"].split("Furnished")[-1].strip().strip(",").strip())

        item_loader.add_value("landlord_phone", "+90.541.504.1885")
        item_loader.add_value("landlord_email", "info@acgexpathomes.com")
        item_loader.add_value("landlord_name", "Acgexpathomes")

        yield item_loader.load_item()