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
    name = 'cnr_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.cnr.com.au/listings/?post_type=listings&count=20&orderby=meta_value&meta_key=dateListed&sold=0&saleOrRental=Rental&paged=1&extended=1&minprice=&maxprice=&minbeds=&maxbeds=&baths=&cars=&type=residential&externalID=&subcategory=&landsize=&order=dateListed-desc", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        data_script = response.xpath("//script[contains(.,'MapDataStore')]/text()").get()
        data = data_script.split("=")[1].split(";")[0].strip()
        data_json = json.loads(data)
        
        for data in data_json:
            follow_url = response.urljoin(data["url"])
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"data":data, "property_type": response.meta.get('property_type')}
            )
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Cnr_Com_PySpider_australia")
        data = response.meta.get("data")
        
        item_loader.add_value("latitude", data["Lat"])
        item_loader.add_value("longitude", data["Long"])
        # item_loader.add_value("external_id", data["id"])
        item_loader.add_value("address", data["address"])
        
        city = data["address"]
        if city and "," in city:
            item_loader.add_value("city", city.split(",")[-1].strip())
        title = response.xpath("//title/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        rent = data["price"]
        if rent:
            rent = rent.replace("/"," ").split(" ")[0].split("P")[0].replace("$","")
            price = int(float(rent))*4
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "AUD")
        zipcode = response.xpath("//script[contains(.,'RexTemplate.postAddress')]/text()").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode.split('.postAddress = "')[-1].split('"')[0].split(" ")[-1].strip())
        room_count = response.xpath("//p[contains(@class,'icon-bed')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//p[contains(@class,'icon-bath')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
            
        description = " ".join(response.xpath("//div[contains(@class,'post-content')]//p//text()").getall())
        if description:
            description = re.sub('\s{2,}', ' ', description.strip())
            item_loader.add_value("description", description)
        
        images = [x.split("'")[1] for x in response.xpath("//div[contains(@class,'slick-slides')]//@style[contains(.,'url')]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        deposit = response.xpath("//strong[contains(.,'bond')]/following-sibling::text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace("$","").strip())
        
        external_id = response.xpath("//strong[contains(.,'ID')]/following-sibling::text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
        
        parking = response.xpath("//p[contains(@class,'icon-bath')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        item_loader.add_value("landlord_name", "C&R Realty")
        item_loader.add_value("landlord_phone", "02 9633 3922")
        item_loader.add_value("landlord_email", "info@cnr.com.au")
        
        yield item_loader.load_item()