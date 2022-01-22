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

class MySpider(Spider):
    name = 'pagepearce_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=Apartment&property_price_from=&property_price_to=",
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=Flat&property_price_from=&property_price_to=",
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=Unit&property_price_from=&property_price_to=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=House&property_price_from=&property_price_to=",
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=Townhouse&property_price_from=&property_price_to=",
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=Villa&property_price_from=&property_price_to=",
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=DuplexSemi-detached&property_price_from=&property_price_to=",
                    "https://pagepearce.com.au/?action=epl_search&property_status=&post_type=rental&property_location=&property_category=Terrace&property_price_from=&property_price_to=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='header-view-details']/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(@class,'next') and contains(@class,'page-numbers')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Pagepearce_Com_PySpider_australia")   
        title = response.xpath("//h3[@class='entry-title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())   
        address = " ".join(response.xpath("//h3[contains(@class,'epl-tab-address')]/span//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
        
        zipcode = response.xpath("//h3[contains(@class,'epl-tab-address')]//span[@class='item-pcode']//text()").get()
        if zipcode:
            item_loader.add_value("zipcode", f"QLD {zipcode}")
        item_loader.add_xpath("city", "//h3[contains(@class,'epl-tab-address')]//span[@class='item-suburb']//text()")
        item_loader.add_xpath("room_count", "//span[@class='icon beds']/span/text()")
        item_loader.add_xpath("bathroom_count", "//span[@class='icon bath']/span/text()")
     
        parking = response.xpath("//span[@class='icon parking']/span/text()[.!='0']").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//li[contains(.,'Parking')]/text()").get()
            if parking:
                item_loader.add_value("parking", True)
      
        balcony = response.xpath("//li[@class='balcony']/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
  
        rent ="".join(response.xpath("//span[@class='page-price-rent']/span/text()").getall())
        if rent:
            rent = rent.split("$")[-1].split("/")[0].replace(",","").strip()
            item_loader.add_value("rent", int(float(rent))*4)
        item_loader.add_value("currency", "AUD")
        desc = " ".join(response.xpath("//div[contains(@class,'epl-section-description')]/div/p//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        available_date = response.xpath("//div[contains(@class,'date-available')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(" from ")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        images = [x for x in response.xpath("//div[contains(@class,'carousel-nav')]//div[@class='carousel-cell']/img/@data-lazy-src").getall()]
        if images:
            item_loader.add_value("images", images)
        map_coord = response.xpath("//div[@id='property-gmap']//div/@data-cord").get()
        if map_coord:
            item_loader.add_value("latitude", map_coord.split(",")[0].strip())
            item_loader.add_value("longitude", map_coord.split(",")[1].strip())
        item_loader.add_value("landlord_name", "Page & Pearce")
        item_loader.add_value("landlord_phone", "07 4727 2400")
        item_loader.add_value("landlord_email", "office@pagepearce.com.au")
        yield item_loader.load_item()