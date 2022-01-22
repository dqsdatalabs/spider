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

class MySpider(Spider):
    name = 'millarestateagents_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'    
    thousand_separator = ','
    scale_separator = '.'       

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "http://www.millarestateagents.co.uk/property-for-rent?q=&st=rent&lp=0&up=0&beds=&radius=0&town=&sta=5&sty=1",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "http://www.millarestateagents.co.uk/property-for-rent?q=&st=rent&lp=0&up=0&beds=&radius=0&town=&sta=5&sty=8&sty=7&sty=6&sty=5&sty=4&sty=3&sty=2&sty=10",
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

        for item in response.xpath("//div[contains(@class,'xPL_property')]/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[contains(.,'NEXT')]/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_id", response.url.split("/")[-1])
        item_loader.add_value("external_source", "Millarestateagents_Co_PySpider_united_kingdom") 
        title = response.xpath("//h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())         
            item_loader.add_value("address", title.strip())         
            item_loader.add_value("zipcode", title.split(",")[-1].strip())         
            item_loader.add_value("city", title.split(",")[-2].strip())         
        item_loader.add_xpath("room_count", "//span[.='Bedrooms']/following-sibling::span[1]/text()")
        item_loader.add_xpath("bathroom_count", "//span[.='Bathrooms']/following-sibling::span[1]/text()")

        available_date = response.xpath("//span[.='Available From']/following-sibling::span[1]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        item_loader.add_xpath("rent_string", "//span[.='Rent']/following-sibling::span[1]/text()")

        energy_label = response.xpath("//span[.='EPC']/following-sibling::span[1]/a[1]/text()").get()
        if energy_label:
            if energy_label[0] in ["A","B","C","D","E","F","G"]:
                item_loader.add_value("energy_label", energy_label[0])

        deposit = response.xpath("//span[.='Deposit']/following-sibling::span[1]/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",",""))
        furnished = response.xpath("//span[.='Furnished']/following-sibling::span[1]/text()").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)
        terrace = response.xpath("//li/text()[contains(.,'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        parking = response.xpath("//li/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        script_map = response.xpath("//script[contains(.,'addMarker(')]/text()").get()
        if script_map:
            latlng = script_map.split("addMarker(")[1].split(")")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
        images = [x for x in response.xpath("//div[@id='xImgSlider']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_value("landlord_name", "Millar Sales & Lettings")
        item_loader.add_value("landlord_phone", "028 9084 3321")
        item_loader.add_value("landlord_email", "gareth.millar@millarestateagents.com")
        yield item_loader.load_item()