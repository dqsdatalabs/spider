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

class MySpider(Spider):
    name = 'keops_carcassonne_com'
    execution_type='testing'
    country='france'
    locale='fr'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "http://www.keops-carcassonne.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=1&lang=fr&tri=d_dt_crea&_=1616141229336",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "http://www.keops-carcassonne.com/recherche,incl_recherche_prestige_ajax.htm?surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=2&lang=fr&tri=d_dt_crea&_=1616141229342"
                ],
                "property_type": "house"
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
        
        for item in response.xpath("//a[@itemprop='url']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

        next_page = response.xpath("//ul[@class='hidden-phone']//li[last()]//@href").getall()
        if next_page:
            yield Request()
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Keops_Carcassonne_PySpider_france")
        
        title = "".join(response.xpath("//h1//text()").getall())
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        address = response.xpath("//h1//text()[2]").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", address.split("(")[1].split(")")[0])
            item_loader.add_value("city", address.split("(")[0].strip())
        
        rent = "".join(response.xpath("(//span[@itemprop='price']/text())[1]").get())
        if rent:
            rent = rent.strip('').split("€", -1)[0].strip('').replace("\xa0","")
            item_loader.add_value("rent", rent.strip(''))
        item_loader.add_value("currency", "EUR")
        
        square_meters = response.xpath("//li[contains(.,'Surface')]/div[2]/text()").get()
        if square_meters:
            square_meters = square_meters.split("m")[0].strip()
            item_loader.add_value("square_meters", square_meters)
        
        room_count = response.xpath("//li[contains(.,'Chambre')]/div[2]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif response.xpath("//li[contains(.,'Pièce')]/div[2]/text()").get():
            item_loader.add_xpath("room_count", "//li[contains(.,'Pièce')]/div[2]/text()")
        
        bathroom_count = response.xpath("//li[contains(.,'Salle')]/div[2]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        external_id = response.xpath("substring-after(//span[@class='bold'][contains(.,'Référence')]/text(),':')").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())
            
        energy_label = response.xpath("//p[contains(.,'Consommations énergétiques')]/following-sibling::div//div[contains(@class,'span2')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())
        
        deposit = response.xpath("//strong[contains(.,'Dépôt')]/text()[not(contains(.,'N/A'))]").get()
        if deposit:
            deposit = deposit.split(":")[1].split(",")[0].strip()
            item_loader.add_value("deposit", deposit)
        utilities = response.xpath("//strong[contains(.,'Honoraires :')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split(".")[0].strip()
            item_loader.add_value("utilities", utilities)
        description = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        latitude_longitude = response.xpath("//script[contains(.,',LATITUDE: \"')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split(',LATITUDE: "')[2].split(',')[0]
            longitude = latitude_longitude.split(',LONGITUDE: "')[2].split('"')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]/div[2]/text()[not(contains(.,'0'))]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'Parking')]/div[2]/text()[not(contains(.,'0'))]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        utilities = response.xpath("substring-after(//li[contains(.,'pour charge')]/text(),':')").get()
        if utilities and utilities.strip():
            utilities = utilities.split("€")[0].strip()
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]/div[2]/text()[.!='0' or contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        furnished = response.xpath("//li[contains(.,'Meublé')]/div[2]/text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li[contains(.,'Ascenseur')]/div[2]/text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        floor = response.xpath("//li[contains(.,'Etage')]/div[2]/text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())
        
        images = [x for x in response.xpath("//div[@id='slider']//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Keops Carcassonne")
        item_loader.add_value("landlord_phone", "04 68 72 00 07")
        
        yield item_loader.load_item()