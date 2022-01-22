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

class MySpider(Spider):
    name = 'placenewmarket_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://placerealestate.com.au/wp-json/api/listings/all?priceRange=&category=Apartment%2CUnit&status=current&type=rental&limit=12&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://placerealestate.com.au/wp-json/api/listings/all?priceRange=&category=House%2CTownhouse%2CVilla&status=current&type=rental&limit=12&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://placerealestate.com.au/wp-json/api/listings/all?priceRange=&category=Studio&status=current&type=rental&limit=12&paged=1&bed=&bath=&car=&sort=",
                ],
                "property_type" : "studio",
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

        data = json.loads(response.body)
        if data["status"] == "SUCCESS":
            for item in data["results"]:
                seen = True
                yield Request(response.urljoin(item["slug"]), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        if page == 2 or seen:
            follow_url = response.url.replace("&paged=" + str(page - 1), "&paged=" + str(page))
            yield Request(follow_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "page": page + 1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Placenewmarket_Com_PySpider_australia")
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("normalize-space(//div[@class='listing-price-inner']/div[contains(@class,'listing-price')]/text())").extract())
        if rent:
            if "week" in rent.lower():
                price = rent.split(" ")[0].replace("\xa0",".").replace(",",".").replace(" ","").replace("$","").strip()
                if price !="NC":
                    item_loader.add_value("rent", int(float(price))*4)
                    item_loader.add_value("currency", "AUD")

        deposit = "".join(response.xpath("normalize-space(//div[@class='listing-price-inner']/div[contains(@class,'listing-status')]/text())").extract())
        if deposit:
            item_loader.add_value("deposit", deposit.split(" ")[1].replace(",","").strip())

        item_loader.add_xpath("room_count", "//div[span[contains(.,'Bed')]]/span[1]/span/text()[.!='0']")
        item_loader.add_xpath("bathroom_count", "//div[contains(@class,'listing-features')]/span[contains(.,'Bath')]/span/text()")

        address = "".join(response.xpath("//div[contains(@class,'listing-address')]//text()").extract())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address.strip()))

        zipcode = "".join(response.xpath("//div[@class='state-postcode ']//text()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split(",")[1].strip())
            item_loader.add_value("city", zipcode.split(",")[0].strip())

        images = [x for x in response.xpath("//div[@class='listing-single-slider']/div/img/@data-lazy").extract()]
        if images:
                item_loader.add_value("images", images)
                  
        latitude = " ".join(response.xpath("substring-before(substring-after(//script[@id='main-map-js-extra']/text(),'lat'),',')").extract())
        if latitude:
            lat = latitude.replace('":"',"").replace('"',"")
            item_loader.add_value("latitude", lat.strip())

        longitude = " ".join(response.xpath("substring-before(substring-after(//script[@id='main-map-js-extra']/text(),'long'),'}')").extract())
        if longitude:
            lng = longitude.replace('":"',"").replace('"',"")
            item_loader.add_value("longitude", lng.strip())

      
        desc = " ".join(response.xpath("//div[contains(@class,'listing-single-content')]/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
 
        parking =response.xpath("//div[contains(@class,'listing-features')]/span[contains(.,'Car ')]/span/text()").extract_first()    
        if parking:
            (item_loader.add_value("parking", True) if parking !="0" else item_loader.add_value("parking", False))

        item_loader.add_xpath("landlord_name", "normalize-space(//div[@class='agent-inner']/div/div[@class='agent-name']/text())")
        item_loader.add_xpath("landlord_phone", "normalize-space(//div[@class='agent-contact']/div/a/text())")

        email = response.xpath("//div[contains(@class,'agent-email')]/a/@href").extract_first()
        if email:
            item_loader.add_value("landlord_email", email.replace("mailto:","").strip())

        yield item_loader.load_item()