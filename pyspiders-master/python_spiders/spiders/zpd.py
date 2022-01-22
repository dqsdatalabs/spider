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
from datetime import datetime


class MySpider(Spider):
    name = "zpd"
    allowed_domains = ["zpd.be"]
    start_urls = ("https://www.zpd.be/zpd-tehuur-woning.asp",
                  "https://www.zpd.be/zpd-tehuur-hob.asp",
                  "https://www.zpd.be/zpd-tehuur-villa.asp",
                  "https://www.zpd.be/zpd-tehuur-hoeve.asp",
                  "https://www.zpd.be/zpd-tehuur-studio.asp",
                  "https://www.zpd.be/zpd-tehuur-appartement.asp",)
    execution_type = "testing"
    country = "belgium"
    locale = "nl"
    thousand_separator = "."
    scale_separator = ","

    def sub_string_between(self, source, s1, s2):
        tmp = source[source.index(s1) + len(s1):]
        return tmp[: tmp.index(s2)]

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url, callback=self.parse)

    def parse(self, response, **kwargs):
        for item_responses in response.xpath(f".//input[@value='lees meer']"):
            link = self.sub_string_between(item_responses.xpath("@onclick").get(), "'", "'")
            if link:
                yield Request(link, self.parse_detail)

    def parse_detail(self, response):
        main_block = f".//body"
        stats = f"{main_block}//table[@class='kader_detail']"
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value(
            "external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale)
        )
        item_loader.add_value("external_id", response.url.split("artikel=")[-1])
        item_loader.add_value("title",
                              self.sub_string_between(response.xpath(".//head/title/text()").get(), "- ", " -"))
        item_loader.add_xpath("images",
                              f"{main_block}//a[@title='Stijlvol nieuwbouw appartement op eerste verdieping']/@href")
        item_loader.add_xpath("floor", f"{stats}//tr[td[contains(.,'Aantal bouwlagen')]]/td[2]/text()")
        item_loader.add_xpath("room_count", f"{stats}//tr[td[contains(.,'Aantal slaapkamers')]]/td[2]/text()")
        item_loader.add_xpath("bathroom_count", f"{stats}//tr[td[contains(.,'Aantal badkamers')]]/td[2]/text()")
        item_loader.add_xpath("images", f".//a[@title='{self.sub_string_between(response.xpath('.//head/title/text()').get(), '- ', ' -')}']/@href")
        item_loader.add_value("rent_string",
                              response.xpath(f"{stats}//tr[td[contains(.,'Prijs') or contains(.,'prijs')]]/td[2]/text()").get().split(self.scale_separator)[0])
        item_loader.add_xpath("square_meters", f"{stats}//tr[td[contains(.,'Bewoonbare oppervlakte ')]]/tr[2]/text()")
        #item_loader.add_xpath("available_date", f"{stats}//tr[td[contains(.,'Beschikbaarheid ')]]/tr[2]/text()")
        item_loader.add_xpath("description", f"{stats}//tr[last()]//text()")
        item_loader.add_value("zipcode", re.search(r"[0-9]{4}", response.xpath(f"{stats}//tr[td[contains(.,'Adres') or contains(.,'adres')]]/td[2]/text()").get()).group())
        item_loader.add_value("city", re.split(r"[0-9]{4}", response.xpath(f"{stats}//tr[td[contains(.,'Adres') or contains(.,'adres')]]/td[2]/text()").get())[-1])
        item_loader.add_xpath("address", f"{stats}//tr[td[contains(.,'Adres') or contains(.,'adres')]]/td[2]/text()")
        item_loader.add_value("landlord_name", "BVBA zakenkantoor Philippe Dhont")
        
        parking = response.xpath("//td[contains(.,'Auto')]/following-sibling::td/text()").get()
        if parking and ("garage" in parking.lower() or "parking" in parking.lower() or "autostandplaats" in parking.lower()):
            item_loader.add_value("parking",True)
        elif parking:
            item_loader.add_value("parking",False)
        
        prep_rent = response.xpath("//td[contains(.,'syndic')]/following-sibling::td/text()").get()
        if prep_rent:
            item_loader.add_value("prepaid_rent", prep_rent.replace("€","").strip())

        available_date = response.xpath("//td[contains(.,'Beschikbaarheid')]/following-sibling::td/text()").get()
        if available_date:
            if available_date.lower().strip() == "onmiddellijk":    
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            elif not available_date.replace(" ","").isalpha():
                date_parsed = dateparser.parse(available_date)
                date3 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date3)
        
        if "rijwoning" in "".join(response.xpath("//p[@class='TitelWoningen']/text()").getall()):
            item_loader.add_value("terrace", True)
        
        if "gemeubileerd" in item_loader.get_collected_values("description"):
            item_loader.add_value("furnished", True)


        if "appartement" in response.xpath(f"{stats}//tr[td[contains(.,'Type') or contains(.,'type')]]/td[2]/text()").get():
            item_loader.add_value("property_type", "apartment")
        else:
            item_loader.add_value("property_type", "house")
        item_loader.add_xpath("deposit", f"{stats}/tr[td[contains(.,'Voorschot syndic ')]]/td[2]/text()")
        if response.xpath(f"{stats}//tr[td[contains(.,'Prijs') or contains(.,'prijs')]]/td[2][contains(.,'€')]/text()"):
            yield item_loader.load_item()
