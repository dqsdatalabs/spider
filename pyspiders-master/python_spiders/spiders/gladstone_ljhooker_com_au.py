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
import re

class MySpider(Spider):
    name = 'gladstone_ljhooker_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://gladstone.ljhooker.com.au/search/unit_apartment-for-rent/page-1?surrounding=true",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://gladstone.ljhooker.com.au/search/house+townhouse+duplex_semi_detached+penthouse+terrace-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://gladstone.ljhooker.com.au/search/studio-for-rent/page-1?surrounding=True&liveability=False",
                ],
                "property_type" : "studio"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'property-content')]/div[@onclick]//a[contains(@class,'property-sticker')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@aria-label='Next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        night = response.xpath("//div[@class='property-heading']/h2/text()").extract_first()
        if "night" in night.lower():
            return
        item_loader.add_value("external_source", "Gladstone_Ljhooker_Com_PySpider_australia")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("title", "//title/text()")

        rent =" ".join(response.xpath("//div[@class='property-heading']/h2/text()[contains(.,'$')]").extract())
        if rent:         
            price =  rent.replace("From","").strip().split(" ")[0].split("$")[1].replace(",","").strip()
            item_loader.add_value("rent",int(float(price))*4)
        item_loader.add_value("currency","AUD")

        item_loader.add_xpath("room_count","//div[@class='property-icon flat-theme']/span[@class='bed ']/text()")
        item_loader.add_xpath("bathroom_count","//div[@class='property-icon flat-theme']/span[@class='bathroom ']//text()")

        address = " ".join(response.xpath("//div[@class='property-heading']/h1//text()").getall())
        if address:
            item_loader.add_value("address", re.sub("\s{2,}", " ", address))
            zipcode =  address.split(",")[1].strip()
            city =  " ".join(address.replace(",","").strip().split(zipcode)[0].strip().split(" ")[-2:]).strip()
            item_loader.add_value("city",city)
            item_loader.add_value("zipcode",zipcode)

        item_loader.add_xpath("latitude","//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude","//meta[@property='og:longitude']/@content")

        desc =  " ".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())

        external_id =  " ".join(response.xpath("//div[@class='code']/text()").extract())
        if external_id:
            ext_id = external_id.split("ID")[1].strip()
            item_loader.add_value("external_id", ext_id)

        images = [ x for x in response.xpath("//div[@class='thumb']/span/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)

        available_date="".join(response.xpath("//ul/li/strong[.='Date Available:']/following-sibling::text()").getall())
        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)


        parking = "".join(response.xpath("//div[@class='property-icon flat-theme']/span[@class='carpot ']//text()").extract())
        if parking:
            (item_loader.add_value("parking", True) if "0" not in parking else item_loader.add_value("parking", False))

        dishwasher = "".join(response.xpath("//div[@class='col-md-7']/ul/li[contains(.,'Dishwasher')]/text()").extract())
        if dishwasher:
            item_loader.add_value("dishwasher", True)

        furnished = "".join(response.xpath("//div[@class='col-md-7']/ul/li[contains(.,'Furnished') or contains(.,'furnished')]/text()").extract())
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished", False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished", True)

        elevator = "".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()[contains(.,'Elevator')]").extract())
        if elevator:
            item_loader.add_value("elevator", True)

        balcony = "".join(response.xpath("//div[@class='property-text is-collapse-disabled']/p/text()[contains(.,'balcony ')]").extract())
        if balcony:
            item_loader.add_value("balcony", True)
        item_loader.add_value("landlord_name", "LJ Hooker Gladstone")
        item_loader.add_value("landlord_phone", "(07) 4976 8000")
        item_loader.add_value("landlord_email", "gladstone@ljh.com.au")

        yield item_loader.load_item()