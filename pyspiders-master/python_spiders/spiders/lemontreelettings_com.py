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
    name = 'lemontreelettings_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    thousand_separator = ','
    scale_separator = '.'

    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://www.lemontreelettings.com/grid?sta=toLet&st=rent&pt=residential&stygrp=3",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.lemontreelettings.com/grid?sta=toLet&st=rent&pt=residential&stygrp=2&stygrp=10&stygrp=8&stygrp=9&stygrp=6",
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

        for item in response.xpath("//div[@class='list-item-content']/h2/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
        
        next_button = response.xpath("//a[@class='paging-next']/@href").get()
        if next_button: yield Request(response.urljoin(next_button), callback=self.parse, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Lemontreelettings_PySpider_united_kingdom")
        item_loader.add_xpath("title", "//title/text()")
        item_loader.add_value("external_id", item_loader.get_collected_values("external_link")[0].split('/')[-1].strip())

        item_loader.add_xpath("address", "normalize-space(//tr[contains(@class,'tbl-keyInfo-address')]/td/text())")

        item_loader.add_xpath("room_count", "normalize-space(//tr[contains(@class,'tbl-keyInfo-bedroom')]/td/text())")
        item_loader.add_xpath("bathroom_count", "normalize-space(//tr[contains(@class,'tbl-keyInfo-bathroom')]/td/text())")
        item_loader.add_xpath("energy_label", "normalize-space(//tr[contains(@class,'tbl-keyInfo-epcRating')]/td/span//text())")

        city = response.xpath("normalize-space(//span[@class='addr-location']/span[@class='addr-town']/text())").extract_first()
        if city:
            item_loader.add_value("city", city.strip())

        zipcode = " ".join(response.xpath("//h1//span[@class='addr-location']/span[@class='addr-postcode']/span//text()").extract())
        if zipcode:
            item_loader.add_value("zipcode", zipcode.strip())

        latitude = response.xpath("//meta[@property='og:latitude']/@content").get()
        if latitude: item_loader.add_value("latitude", latitude.strip())

        longitude = response.xpath("//meta[@property='og:longitude']/@content").get()
        if longitude: item_loader.add_value("longitude", longitude.strip())

        rent = response.xpath("//tr[contains(@class,'tbl-keyInfo-rent')]/td/text()").get()
        term = response.xpath("//tr[contains(@class,'tbl-keyInfo-rent')]/td/span/text()").get()
        if rent and term:
            rent = "".join(filter(str.isnumeric, rent.replace(',','')))
            if "month" in term.lower(): term = 1
            elif "week" in term.lower(): term = 4
            elif "day" in term.lower(): term = 30
            else: term = 1
            item_loader.add_value("rent", int(rent) * term)
            item_loader.add_value("currency", "GBP")
        
        deposit = response.xpath("//tr[contains(@class,'tbl-keyInfo-deposit')]/td/text()").get()
        if deposit:  
            deposit = "".join(filter(str.isnumeric, deposit.split('.')[0]))
            if term: item_loader.add_value("deposit", int(deposit))
            else: item_loader.add_value("deposit", deposit)

        description = " ".join(response.xpath("//section[@class='listing-additional-info']//text()").getall())  
        if description:
            item_loader.add_value("description", description.strip())

        available_date="".join(response.xpath("//tr[contains(@class,'tbl-keyInfo-availableFrom')]/td/text()").getall())

        if available_date:
            date2 =  available_date.strip()
            date_parsed = dateparser.parse(
                date2, date_formats=["%m-%d-%Y"]
            )
            date3 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date3)

        images = [x for x in response.xpath("//div[@class='container-fluid listing-slideshow']//a/@href[not(contains(.,'=video') or contains(.,'=tour'))]").getall()]
        if images:
            item_loader.add_value("images", images)

        furnished = response.xpath("normalize-space(//tr[contains(@class,'tbl-keyInfo-furnished')]/td/text())").get()
        if furnished:
            if "unfurnished" in furnished.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in furnished.lower():
                item_loader.add_value("furnished",True)

        item_loader.add_value("landlord_phone", "028 7083 1113")
        item_loader.add_value("landlord_name", "LEMONTREE LETTINGS")
        item_loader.add_value("landlord_email", "office@lemontreelettings.com")

        yield item_loader.load_item()