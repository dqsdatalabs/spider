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
    name = 'brenner_immo_de'
    execution_type='testing'
    country = 'germany'
    locale ='de'
    external_source = "Brenner_Immo_PySpider_germany"
    def start_requests(self):

        start_urls = [
            {
                "url" : [
                    "https://brenner-immo.de/immobilien/?post_type=immomakler_object&vermarktungsart=miete&nutzungsart&typ=wohnung&ort&center&radius=500&objekt-id&collapse&von-qm=0.00&bis-qm=305.00&von-zimmer=0.00&bis-zimmer=8.00&von-kaltmiete=0.00&bis-kaltmiete=1400.00",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://brenner-immo.de/immobilien/?post_type=immomakler_object&vermarktungsart=miete&nutzungsart&typ=haus&ort&center&radius=500&objekt-id&collapse&von-qm=0.00&bis-qm=305.00&von-zimmer=0.00&bis-zimmer=8.00&von-kaltmiete=0.00&bis-kaltmiete=1400.00",
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

        for url in response.xpath("//h3[@class='property-title']//@href").getall():
            yield Request(url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_xpath("external_id", "//div[div[text()='Objekt ID']]/div[2]/text()")
     
        item_loader.add_value("external_source", self.external_source)          
        item_loader.add_xpath("title","//h1/text()")
        item_loader.add_xpath("room_count", "//div[div[text()='Schlafimmer']]/div[2]/text()[normalize-space()]")
        item_loader.add_xpath("bathroom_count", "//div[div[text()='Badezimmer']]/div[2]/text()[normalize-space()]")

        address = "".join(response.xpath("//div[div[text()='Adresse']]/div[2]/text()[normalize-space()]").getall())
        if address:
            address = address.replace("\u00a0"," ").strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split(" ")[0].strip())
            item_loader.add_value("city", address.split(" ")[-1].strip())
  
        available_date = response.xpath("//div[div[text()='Verfügbar ab']]/div[2]/text()[normalize-space()]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        rent = "".join(response.xpath("//div[div[text()='Kaltmiete']]/div[2]/text()[normalize-space()]").getall()) 
        if rent:
            item_loader.add_value("rent_string", rent.replace(",00",""))
        square_meters = "".join(response.xpath("//div[div[contains(.,'Wohnfl')]]/div[2]/text()[normalize-space()]").getall()) 
        if square_meters:
            item_loader.add_value("square_meters", square_meters.split(",")[0].split("m")[0])
        deposit = "".join(response.xpath("//div[div[text()='Kaution']]/div[2]/text()[normalize-space()]").getall()) 
        if deposit:
            item_loader.add_value("deposit", deposit.replace(",00","").replace(".",""))

        parking = response.xpath("//li//text()[contains(.,'garage') or contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li//text()[contains(.,'Balkon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        energy_label = response.xpath("//div[div[text()='Energie­effizienz­klasse']]/div[2]/text()[normalize-space()]").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        description = " ".join(response.xpath("//div[contains(@class,'property-description ')]//text()[normalize-space()]").getall()) 
        if description:
            item_loader.add_value("description", description.strip())
       
        images = [x for x in response.xpath("//div[@id='immomakler-galleria']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        item_loader.add_xpath("landlord_name", "//div[contains(@class,'property-contact')]//div[text()='Name']/following-sibling::div//text()")
        item_loader.add_xpath("landlord_phone", "//div[contains(@class,'property-contact')]//div[text()='Tel. Zentrale']/following-sibling::div//text()")
        item_loader.add_xpath("landlord_email", "//div[contains(@class,'property-contact')]//div[text()='E-Mail Zentrale']/following-sibling::div//text()")

        yield item_loader.load_item()