# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy.spiders import Rule
from scrapy.linkextractors import LinkExtractor
from python_spiders.items import ListingItem
from w3lib.html import remove_tags
from scrapy import Request
from python_spiders.loaders import ListingLoader
import dateparser
import re

class MySpider(Spider):
    name = "jj_properties"
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source = "Jjproperties_PySpider_belgium_fr"
    def start_requests(self):
        start_urls = [
            {"url": "https://www.jj-properties.be/fr/a-louer/appartements", "property_type": "apartment"},
             {"url": "https://www.jj-properties.be/fr/a-louer/maisons", "property_type": "house"}
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                                callback=self.parse,
                                meta={'property_type': url.get('property_type'),
                                        'base_url':url.get('url')})
    # 1. FOLLOWING LEVEL 1
    def parse(self, response):
        
        base_url = response.meta.get("base_url")
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath(
            "//div[@id='search_results__list']/ul/li/div[@class='property_card']/a/@href"
        ).extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, 
                            callback=self.populate_item,
                            meta={'property_type': response.meta.get('property_type')})
            seen = True

        if page == 2 or seen:
            url = base_url + f"/page-{page}"
            yield Request(url, 
                            callback=self.parse, 
                            meta={"page": page + 1, 
                                    "base_url":base_url,
                                    'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        title = response.xpath("//h1//text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        desc = " ".join(
            response.xpath(
                "//div[@class='detail_desc']//text()[not(parent::h3)]"
            ).extract()
        ).strip().replace("\u00e0","")
        if desc:
            item_loader.add_value("description", re.sub('\s{2,}', ' ', desc))

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("description", desc)

        rent = response.xpath(
            "//div[@class='detail_meta']/p/text()[contains(., '€')]"
        ).extract_first()
        if rent:
            item_loader.add_value("rent_string", rent.split("p/m")[0])

        ref = response.xpath(
            "//section[@class='container container--s margin-v']/p/text()"
        ).get()
        if ref:
            item_loader.add_value("external_id", ref.split(" ")[1])

        square = response.xpath("substring-before(//div[dt[.='Surface habitable:']]/dd/text(), ',')").get()
        if square:

            item_loader.add_value("square_meters", square)

        room = response.xpath(
            "//ul/li/span[contains(@class,'icon_bedroom')]/text()"
        ).get()
        if room:
            item_loader.add_value("room_count", room)
        elif response.xpath("//h1[@class='detail_title']/span/text()[contains(.,'Studio')]").get():
            item_loader.add_value("room_count","1")
            
        available_date = response.xpath(
            "//div[dt[.='Disponibilité:']]/dd/text()[2]"
        ).extract_first()
        if available_date:
            date_parsed = dateparser.parse(
                available_date, date_formats=["%d %B %Y"]
            )
            date2 = date_parsed.strftime("%Y-%m-%d")
            item_loader.add_value("available_date", date2)

        item_loader.add_xpath("floor", "//div[dt[.='Etage:']]/dd/text()")

        images = response.xpath("//ul[@id='detail_slider']/li/a/@href").extract()
        for x in range(1, len(images)):
            item_loader.add_value("images", response.urljoin(images[x]))
            
        item_loader.add_value(
            "property_type", response.meta.get("property_type")
        )

        terrace = response.xpath("//div[dt[.='Terrasse:']]/dd/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        terrace = response.xpath("//div[dt[.='Ascenseur:']]/dd/text()").get()
        if terrace:
            if "Oui" in terrace:
                item_loader.add_value("elevator", True)
            elif "Non" in terrace:
                item_loader.add_value("elevator", False)
            elif "Yes" in terrace:
                item_loader.add_value("elevator", True)
            elif "No" in terrace:
                item_loader.add_value("elevator", False)

        parking = response.xpath(
            "//li/span[@class='icon icon_feature-l icon_garages text']"
        ).get()
        if parking:
            item_loader.add_value("parking", True)
            
        washing_machine=response.xpath(
            "//div[@class='detail_desc']//text()[not(parent::h3)][contains(.,'Machine à laver') or contains(.,'machine à laver')]"
            ).get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        if "lave-vaisselle" in desc.lower():
            item_loader.add_value("dishwasher", True)
        if response.xpath("//h3/text()[contains(.,'meublé')]").get():
            item_loader.add_value("furnished", True)
            
        phone = response.xpath(
            "//div[@class='representative representative--small']//p[@class='representative__phone']/span[@class='icon icon_phone text']/text()"
        ).get()
        if phone:
            item_loader.add_value("landlord_phone", phone)
        landlord_name = response.xpath(
            "//div[@class='representative representative--small']//p[@class='representative__name']/text()"
        ).get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)
        item_loader.add_value("landlord_email", "info@jj-properties.be")
        
        item_loader.add_xpath(
            "city", "//label[@class='detail_meta__address']/span/text()"
        )
        
        address = response.xpath("//p[@class='detail_meta__title']/text()").get()
        if address:
            item_loader.add_value("address", address)
            
        latitude_longitude=response.xpath("//script[contains(.,'lat')]/text()").get()
        if latitude_longitude:
            latitude=latitude_longitude.split("lat =")[1].split(";")[0].strip()
            longitude=latitude_longitude.split("lng =")[1].split(";")[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        utilities=response.xpath(
            "//div[contains(@class,'details')]/dl/div/dt[contains(.,'Coûts communs')]/following-sibling::dd/text()"
        ).get()
        if utilities:
            item_loader.add_value("utilities", utilities.split("p/m")[0].replace("€","").strip())
        
        bathroom=response.xpath("//ul/li/span[contains(@class,'icon_bathroom')]/text()").get()
        if bathroom:
            item_loader.add_value("bathroom_count", bathroom)
        
        energy_label=response.xpath("//span[contains(@class,'epc_label')]/@class").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split("class_")[1].strip().upper())
        
        yield item_loader.load_item()
