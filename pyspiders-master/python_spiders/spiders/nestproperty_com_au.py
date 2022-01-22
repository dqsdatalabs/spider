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
    name = 'nestproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    external_source = "Nestproperty_Com_PySpider_australia"
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=apartment&status=for-rent",
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=terrace&status=for-rent",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=duplexsemi-detached&status=for-rent",
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=house&status=for-rent",
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=townhouse&status=for-rent",
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=unit&status=for-rent",
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=villa&status=for-rent",
                ],
                "property_type" : "house",
            },
            {
                "url" : [
                    "https://hobart.nestproperty.com.au/properties-search/page/{}/?type=studio&status=for-rent",
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
        for item in response.xpath("//h3/a/@href").getall():
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
        property_type = response.meta.get('property_type')
        room_type = response.xpath("//h1/text()").extract_first()
        if room_type and "Room" in room_type:
            property_type = "room"
        item_loader.add_value("property_type", property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "substring-after(//link[@rel='shortlink']/@href,'?p=')")
        
        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")

        rent = "".join(response.xpath("//p[@class='price']/text()").extract())
        if rent:
            if "week" in rent:
                price =  rent.strip().split(" ")[0].split(".")[0].split("$")[1].strip()
                item_loader.add_value("rent",int(price)*4)
                item_loader.add_value("currency","AUD")
            else:
                price =  rent.strip().split(" ")[0].split(".")[0].split("$")[1].strip()
                item_loader.add_value("rent",int(price)*4)
                item_loader.add_value("currency","AUD")

        item_loader.add_xpath("address","normalize-space(//h1[@class='rh_page__title']/text())")
        item_loader.add_xpath("room_count","//div[@class='rh_property__meta']/h4[contains(.,'Bedroom')]/following-sibling::div/span/text()")
        item_loader.add_xpath("bathroom_count","//div[@class='rh_property__meta']/h4[contains(.,'Bathroom')]/following-sibling::div/span/text()")
        item_loader.add_xpath("deposit","//li[span[.='Bond:']]/span[2]/text()")
        item_loader.add_xpath("city","//nav/ul/li/a/text()[.!='Home']")

        available_date=response.xpath("//li[span[.='Available From:']]/span[2]/text()").get()
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        desc = " ".join(response.xpath("//div[@class='rh_content']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        meters = " ".join(response.xpath("//div[@class='rh_property__meta']/h4[contains(.,'Area')]/following-sibling::div/span/text()").extract())
        if meters:
            s_meters = meters.strip().split("m²")[0].strip()
            item_loader.add_value("square_meters", s_meters.strip())

        images = [x for x in response.xpath("//ul[@class='slides']/li/a/@href").extract()]
        if images:
            item_loader.add_value("images", images)

        latitude = " ".join(response.xpath("substring-before(substring-after(//script[@id='property-google-map-js-extra']/text(),'lat'),',')").extract())
        if latitude:
            lat = latitude.replace('":"',"").replace('"',"")
            item_loader.add_value("latitude", lat.strip())

        longitude = " ".join(response.xpath("substring-before(substring-after(//script[@id='property-google-map-js-extra']/text(),'lng'),',')").extract())
        if longitude:
            lng = longitude.replace('":"',"").replace('"',"")
            item_loader.add_value("longitude", lng.strip())

        floor_plan_images = [x for x in response.xpath("//div[@class='floor-plan-map']/a/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        parking = "".join(response.xpath("//div[@class='rh_property__meta']/h4[contains(.,'Parking')]/following-sibling::div/span/text()").extract())   
        if parking:
            if parking !="0":
                item_loader.add_value("parking", True)
            else:
                item_loader.add_value("parking", True)

        item_loader.add_xpath("landlord_name", "normalize-space(//input[@name='author_name']/@value)")
        item_loader.add_xpath("landlord_phone", "//p[@class='contact office']/a/text()")
        item_loader.add_xpath("landlord_email", "//p[@class='contact email']/a/text()")
                
        yield item_loader.load_item()
