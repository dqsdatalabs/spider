# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'immobilier_heurtier_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.immobilier-heurtier.fr/annonces?id_polygon=&localisation_etendu=0&visite_virtuelle=&categorie=location&type_bien=appartement&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.immobilier-heurtier.fr/annonces?id_polygon=&localisation_etendu=1&visite_virtuelle=&categorie=location&type_bien=maison&nb_pieces=&surface=&budget=&localisation=&submit=Rechercher", 
                "property_type": "house"
            },
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//p[@class='lien-detail']/a/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
        
        next_page = response.xpath("//div[@class='pagelinks-next']/a/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Immobilierheurtier_PySpider_france")

        external_id = response.xpath("//p[contains(@class,'header-ref')]//text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)

        title = response.xpath("//div[contains(@class,'head-resume')]//h3//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//li[contains(.,'Ville')]//strong//text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address)
        
        zipcode = response.xpath("//li[contains(.,'Code postal')]//strong//text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        square_meters = response.xpath("//p[contains(@class,'surface')]//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split("m")[0].split(".")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//span[@itemprop='price']//text()").get()
        if rent:
            rent = rent.strip().replace(" ","")
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")

        deposit = response.xpath("//li[contains(.,'Dépot de garantie')]//strong//text()").get()
        if deposit:
            deposit = deposit.split("€")[0].split(".")[0].replace(" ","").strip()
            item_loader.add_value("deposit", deposit)

        desc = " ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//li[contains(.,'chambre')]//strong//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[contains(.,'pièce')]//strong//text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.strip())

        bathroom_count = response.xpath("//li[contains(.,'salle')]//strong//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip().split(" ")[0]
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@id,'photoslider')]//@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        from datetime import datetime 
        import dateparser
        available_date = "".join(response.xpath("//li[contains(.,'Disponibilité')]//strong//text()").getall())
        if available_date:
            if not "immédiatement" in available_date.lower():
                date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        availablecheck=item_loader.get_output_value("available_date")
        if not availablecheck:
            available_date = item_loader.get_output_value("description")
            if available_date:
                    available_date=available_date.split("disponible le")[-1].split(".")[0]
                    date_parsed = dateparser.parse(available_date.strip(), date_formats=["%d/%m/%Y"])
                    if date_parsed:
                        date2 = date_parsed.strftime("%Y-%m-%d")
                        item_loader.add_value("available_date", date2)
        utilities=" ".join(response.xpath("//p[@itemprop='description']//text()").getall())
        if utilities:
            utilities=utilities.split("Charge")[-1].split("€")[0].split(".")[0].replace(":","").strip()
            item_loader.add_value("utilities",utilities)


        parking = response.xpath("//li[contains(.,'garage') or contains(.,'parking')]//strong//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcon')]//strong//text()[contains(.,'oui')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//li[contains(.,'Terrasse')]//strong//text()[contains(.,'oui')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//li[contains(.,'Meublé')]//strong//text()[contains(.,'oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]//strong//text()[contains(.,'oui')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//li[contains(.,'Etage')]//strong//text()").get()
        if floor:
            item_loader.add_value("floor", floor.strip())

        energy_label = response.xpath("//div[contains(@class,'diagnostic_images')]//@srcset[contains(.,'dpe')][not(contains(.,'vierge'))]").get()
        if energy_label:
            energy_label = energy_label.split("/dpe/")[1].split("/")[0]
            item_loader.add_value("energy_label", energy_label)
            
        swimming_pool = response.xpath("//li[contains(.,'Piscine')]//strong//text()[contains(.,'oui')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        latitude_longitude = response.xpath("//script[contains(.,' latitude ')]//text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('latitude =')[1].split(';')[0]
            longitude = latitude_longitude.split('longitude =')[1].split(';')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        item_loader.add_value("landlord_name", "Le Cabinet HEURTIER")
        item_loader.add_value("landlord_phone", "04 76 87 78 62")
        
        yield item_loader.load_item()