# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser
from word2number import w2n
import re

class MySpider(Spider):
    name = "housingxl_nl"
    execution_type = 'testing'
    country = 'netherlands'
    locale = 'nl' 

    def start_requests(self):
        start_urls = [
            {
                "url" : "https://www.housingxl.nl/nl/huurwoningen/nederland/kamer/",
                "property_type" : "room"
            },
            {
                "url" : "https://www.housingxl.nl/nl/huurwoningen/nederland/studio/",
                "property_type" : "studio"
            },
            {
                "url" : "https://www.housingxl.nl/nl/huurwoningen/nederland/bovenwoning/",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.housingxl.nl/nl/huurwoningen/nederland/appartement/",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.housingxl.nl/nl/huurwoningen/nederland/tussenwoning/",
                "property_type" : "house"
            },
            {
                "url" : "https://www.housingxl.nl/nl/huurwoningen/nederland/vrijstaande-woning/",
                "property_type" : "house"
            },
        ]# LEVEL 1
        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING LEVEL 1
    def parse(self, response):

        for follow_url in response.xpath("//article//h2/a[not(contains(@href,'javascript'))]/@href").extract():
            yield response.follow(follow_url, self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@title='next']/@href").get()
        if next_page and "javascript" not in next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type')}
            )

    # 2. SCRAPING LEVEL 2

    def populate_item(self, response):
        item_loader =ListingLoader(response=response)

        item_loader.add_value("external_source", "Housingxl_PySpider_" + self.country + "_" + self.locale)

        title = response.xpath("//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)

        item_loader.add_value("external_link", response.url)

        item_loader.add_xpath(
            "external_id", "//ul[@class='details']//li[./span[.='ID']]/span[2]/text()")
        desc = "".join(response.xpath(
            "//div[@class='description']/p[2]/text()").extract())
        item_loader.add_value("description", desc)

        if desc:
            if 'badkamer' in desc.lower():
                try:
                    bathroom_count = w2n.word_to_num(desc.lower().split('badkamer')[0].strip().split(' ')[-1].strip())
                    item_loader.add_value("bathroom_count", str(bathroom_count))
                except:
                    pass
            if 'balkon' in desc.lower():
                item_loader.add_value("balcony", True)
            if 'lift' in desc.lower():
                item_loader.add_value("elevator", True)
            if 'terras' in desc.lower():
                item_loader.add_value("terrace", True)
            if 'zwembad' in desc.lower():
                item_loader.add_value("swimming_pool", True)
            if 'vaatwasser' in desc.lower():
                item_loader.add_value("dishwasher", True)
            if 'wasmachine' in desc.lower():
                item_loader.add_value("washing_machine", True)
        
        furnished = response.xpath("//span[contains(.,'Gemeubileerd')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_xpath(
            "city", "//ul[@class='details']//li[./span[.='Plaats']]/span[2]/text()")
        item_loader.add_xpath(
            "zipcode", "//ul[@class='details']//li[./span[.='Postcode']]/span[2]/text()")
        item_loader.add_xpath(
            "address", "//aside[contains(@class, 'actions')]/p[1]/text()")
        
        item_loader.add_value("property_type", response.meta.get('property_type'))

        square = response.xpath(
            "//ul[@class='details']//li[./span[.='Oppervlakte woning']]/span[2]/text()").get()
        item_loader.add_value("square_meters", square.split("m2")[0])

        item_loader.add_xpath(
            "room_count", "//ul[@class='details']//li[./span[.='Aantal kamers']]/span[2]/text()")

        available_date = response.xpath("//ul[@class='details']//li[./span[.='Beschikbaar vanaf']]/span[2]/text()[. !='Direct beschikbaar']").extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        images = [response.urljoin(x)for x in response.xpath("//div[@class='image-small image-slider-thumbs row']/div/@data-src").extract()]
        if images:
            item_loader.add_value("images", list(set(images)))
            item_loader.add_value("external_images_count", len(list(set(images))))

        price = response.xpath("//ul[@class='details']/li/span[.='Prijs']/following-sibling::span/text()").extract_first()
        if price:
            item_loader.add_value("rent", price.split("€")[1].split("p")[0])
            item_loader.add_value("currency", "EUR")

        item_loader.add_xpath(
            "deposit", "//ul[@class='details']/li/span[.='Borgsom']/following-sibling::span/text()")

        terrace = response.xpath("//div[@class='amenities']/ul/li[not(self::li[@class='disabled'])]/span[@class='icon-checkmark']/following-sibling::span/text()[contains(.,'Parkeergelegenheid')]").get()
        if terrace:
            item_loader.add_value("parking", True)

        pets_allowed = response.xpath("//li[@class='disabled']//span[contains(.,'Huisdieren toegestaan')]").get()
        if pets_allowed:
            item_loader.add_value("pets_allowed", False)
        else:
            item_loader.add_value("pets_allowed", True)

        item_loader.add_value("landlord_phone", "0031(0)43-7600076")
        item_loader.add_value("landlord_email", "maastricht@housingxl.nl")
        item_loader.add_value("landlord_name", "HousingXL")

        yield item_loader.load_item()

