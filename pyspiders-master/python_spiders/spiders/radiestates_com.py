# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'radiestates_com'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.radiestates.com/search-results/?list=lease&keywords=&property_type=Unit&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=&email_address=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.radiestates.com/search-results/?list=lease&keywords=&property_type=House&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=&email_address=",
                    "https://www.radiestates.com/search-results/?list=lease&keywords=&property_type=Villa&price_min=&price_max=&bedrooms=&bathrooms=&carspaces=&email_address="
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
        for item in response.xpath("//div[@id='listings']//div[contains(@class,'listing')]//div[contains(@class,'slider')]/div[1]/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Radiestates_PySpider_australia")
        title = response.xpath("//h3[@class='section-title address']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
            item_loader.add_value("address", title.strip())

        item_loader.add_xpath("city", "//input[@id='papf_propsub']/@value")
        zipcode = response.xpath("//input[@id='papf_propstat']/@value").get()
        zipcode1 = response.xpath("//input[@id='papf_proppc']/@value").get()
        if zipcode:
            item_loader.add_value("zipcode", zipcode+" "+zipcode1)
        item_loader.add_xpath("room_count","//li[@class='bedrooms']/text()")
        item_loader.add_xpath("bathroom_count","//li[@class='bathrooms']/text()")
        rent = response.xpath("//li[@class='price']/span[@class='value']/text()").get()
        if rent:
            rent = "".join(filter(str.isnumeric, rent.replace(",","")))
            item_loader.add_value("rent", str(int(float(rent)*4)))
        item_loader.add_value("currency", "AUD")
      
        parking = response.xpath("//li[@class='carspaces']/text()").get()
        if parking:
            if parking.strip() =="0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
      
        description = " ".join(response.xpath("//div[@id='full_property_description']/p//text()[.!='[Less]']").getall())
        if description:
            item_loader.add_value("description", description.strip())
      
        latlng = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if latlng:
            item_loader.add_value("latitude", latlng.split("L.marker([")[1].split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split("L.marker([")[1].split(",")[1].split("]")[0].strip())
        images = [x for x in response.xpath("//div[@id='slick-content']//a/img/@src").getall() if x]
        if images:
            item_loader.add_value("images", images)
    
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'agent agent-0')]//p[@class='name']/strong/text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'agent agent-0')]//p[@class='contact phone']/text()")
        item_loader.add_value("landlord_email", "info@radiestates.com")
        yield item_loader.load_item()