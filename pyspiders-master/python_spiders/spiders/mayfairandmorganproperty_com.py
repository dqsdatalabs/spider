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
    name = 'mayfairandmorganproperty_com_disabled'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    external_source='Mayfairandmorganproperty_PySpider_united_kingdom'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.mayfairandmorganproperty.com/property-to-rent",
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

        for item in response.xpath("//ul[@class='property-list']/li/div/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mayfairandmorganproperty_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_xpath("city", "//span[@class='address-other']/span[@class='locality']/text()")
        item_loader.add_xpath("latitude", "//meta[@property='og:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='og:longitude']/@content")
        item_loader.add_value("external_id", item_loader.get_collected_values("external_link")[0].split('/')[-1].strip())

  
        item_loader.add_xpath("address", "normalize-space(//tr[contains(@class,'tbl-keyInfo-address')]/td/text())")
        item_loader.add_xpath("rent_string", "normalize-space(//tr[contains(@class,'tbl-keyInfo-rent')]/td/text())")

        item_loader.add_xpath("room_count", "normalize-space(//tr[contains(@class,'tbl-keyInfo-bedroom')]/td/text())")
        item_loader.add_xpath("bathroom_count", "normalize-space(//tr[contains(@class,'tbl-keyInfo-bathroom')]/td/text())")
        item_loader.add_xpath("energy_label", "normalize-space(//tr[contains(@class,'tbl-keyInfo-epcRating')]/td/span//text())")

        city = response.xpath("normalize-space(//span[@class='addr-location']/span[@class='addr-town']/text())").extract_first()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = " ".join(response.xpath("//h1//span[@class='addr-location']/span[@class='addr-postcode']/span//text()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())
        else:
            zipcode = " ".join(response.xpath("//span[@class='address-other']/span[@class='postcode']/text()").extract())
            if zipcode:
                item_loader.add_value("zipcode", zipcode.replace(",","").strip())
        deposit = "".join(response.xpath("//tr[contains(@class,'tbl-keyInfo-deposit')]/td/text()").extract())
        if deposit:          
            item_loader.add_value("deposit", deposit.strip())

        description = " ".join(response.xpath("//section[@class='listing-additional-info']//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())
        item_loader.add_xpath("latitude", "//div[@class='directions-map']/@data-lat")
        item_loader.add_xpath("longitude", "//div[@class='directions-map']/@data-lng")

        available_date="".join(response.xpath("//tr[contains(@class,'tbl-keyInfo-availableFrom')]/td/text()").getall())

        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//div[@class='property-slider ']//a/@href[not(contains(.,'=video') or contains(.,'=tour'))]").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("normalize-space(//tr[contains(@class,'tbl-keyInfo-furnished')]/td/text())").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "028 9446 4902")
        item_loader.add_value("landlord_name", "MAYFAIR & MORGAN")
        item_loader.add_value("landlord_email", "info@mayfairandmorgan.com")

        yield item_loader.load_item()