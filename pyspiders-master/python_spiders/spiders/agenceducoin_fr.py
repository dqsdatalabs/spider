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
    name = 'agenceducoin_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.agenceducoin.fr/fr/liste.htm?menu=5&page=1#menuSave=5&page=1&ListeViewBienForm=text&ope=2",
                ],
                "property_type": "apartment"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
                
        for item in response.xpath("//a[contains(@class,'ico-more ico-gray')]"):
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(
                follow_url,
                callback=self.populate_item,
                meta={"property_type": response.meta.get('property_type')}
            )
        
        next_page = response.xpath("//span[@class='PageSui']/span/@id").get()
        if next_page:
            yield Request(
                response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type": response.meta.get('property_type')}
            )

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Agenceducoin_PySpider_france")
        
        title = response.xpath("//h2/text()").get()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            item_loader.add_value("city", title.split("(")[0].strip())
            item_loader.add_value("zipcode", title.split("(")[1].split(")")[0].strip())
        
        square_meters = response.xpath("//span[contains(@class,'surface')]/following-sibling::text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//span[contains(@class,'chambre')]/following-sibling::text()[not(contains(.,'NC'))]").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip().split(" ")[0])
        else:
            room_count = response.xpath("//span[contains(@class,'piece')]/following-sibling::text()[not(contains(.,'NC'))]").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip().split(" ")[0])
                
        rent = response.xpath("//div[@class='detail-bien-prix']/text()").get()
        if rent:
            price = rent.split("€")[0].replace(" ","").replace(",",".")
            item_loader.add_value("rent", int(float(price)))
            item_loader.add_value("currency", "EUR")
        
        external_id = response.xpath("//span[@itemprop='productID']/text()").getall()
        if external_id:
            item_loader.add_value("external_id", external_id[1])
        
        description = " ".join(response.xpath("//div[contains(@class,'detail-bien-desc-')]//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        

        if "Disponibilité le" in description:
            available_date = description.split("Disponibilité le")[1].split("!")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        if "studio" in description.lower():
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))
        
        images = [x for x in response.xpath("//div[contains(@class,'large-flap-container')]//@src[not(contains(.,'anti-cheat'))]").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//li[@class='gg-map-marker-lat']/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
            
        longitude = response.xpath("//li[@class='gg-map-marker-lng']/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude)
        
        energy_label = response.xpath("//input[@id='DetailNrjNote']/@value").get()
        item_loader.add_value("energy_label", energy_label)
        
        utilities = response.xpath("//span[@class='cout_charges_mens']/text()").get()
        item_loader.add_value("utilities", int(float(utilities.replace(",","."))))
        
        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]/following-sibling::span[@class='cout_honoraires_loc']/text()[.!='0']").get()
        if deposit:
            item_loader.add_value("deposit", deposit.strip().replace(" ","."))
        
        item_loader.add_value("landlord_name", "L'AGENCE DU COIN")
        item_loader.add_value("landlord_phone", "04 48 19 40 40")
        
        yield item_loader.load_item()