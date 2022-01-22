# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'immobilier_vitrolles_solvimo_com'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Immobilier_Vitrolles_Solvimo_PySpider_france"
    def start_requests(self):
        start_urls = [
            {"url": "https://immobilier-vitrolles.nestenn.com/?agence_name_hidden=1609416888&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Appartement&type=Appartement&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "apartment"},
            {"url": "https://immobilier-vitrolles.nestenn.com/?agence_name_hidden=1609416892&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Maison&type=Maison&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "house"},
            {"url": "https://immobilier-vitrolles.nestenn.com/?agence_name_hidden=1609417413&prestige=0&action=listing&transaction=Location&list_ville=&list_type=Studio&type=Studio&prix_max=&pieces=&chambres=&surface_min=&surface_max=&surface_terrain_min=&surface_terrain_max=&ref=", "property_type": "studio"}
            
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):

         for item in response.xpath("//div[@id='bienParent1']/div[@id='annonce_entete_right']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            
# 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        rented = response.xpath("//div[@class='content_prix']/text()[contains(.,'Loué')]").get()
        if rented: return

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_xpath("title", "//title/text()")

        rent = response.xpath("//div[@class='content_prix']/text()[not(contains(.,'Loué'))]").get()
        if rent:
            if "Vendu" not in rent:
                item_loader.add_value("rent_string", rent)
        else:
            item_loader.add_value("currency", "EUR")
        square_meters=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'habitable')]/text()").get()
        if square_meters:
            meters = square_meters.split('m²')[0].strip().replace(",",".")
            item_loader.add_value("square_meters",int(float(meters)) )
        
        room_count=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'chambre')]/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split(' ')[0])
        else:
            room_count=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'piece')]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count.split(' ')[0])


        deposit=response.xpath("//p[@class='textAlign_C']/text()[contains(.,'Dépôt de garantie') and contains(.,'€') ]").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(':')[1])

        external_id=response.xpath("substring-after(//div[@id='ref']/text(),'Réf : ')").get()
        if deposit:
            item_loader.add_value("deposit", deposit.split(':')[1])

        bathroom_count=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'salle d eau') or contains(.,'salles d eau') or contains(.,'salle de bain') ]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('salle')[0].strip())
        
        # adres = ""
        # zipcode = ""
        # city = ""
        address="".join(response.xpath("//div[@class='ariane_pc flex flexAC'][3]/a/text()").getall())
        if address:
            zipcode = address.split("(")[1].split(")")[0].strip()
            city = address.split("(")[0].strip().split(" ")[-1].strip()
            item_loader.add_value("zipcode", zipcode.strip())
            item_loader.add_value("city", city)
            item_loader.add_value("address","{} {}".format(zipcode,city) )

        
        latitude_longitude = response.xpath("//script[contains(.,'lngLat')]/text()").get()
        if latitude_longitude:
            longitude = latitude_longitude.split('center: [')[1].split(',')[0]
            latitude = latitude_longitude.split('center: [')[1].split(',')[1].split(']')[0]
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        external_id=response.xpath("//div[@id='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split('Réf :')[1].strip())

        energy_label=response.xpath("//div[@class='dpeBoxMini']/div[@id='consoEner_a']/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.strip())

        desc="".join(response.xpath("//div[@class='container']/p[1]/text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())

        if "provision" in desc:
            try:
                utilities = desc.split("provision")[0].split("euro")[-2].strip().split(" ")[-1]
                if utilities !='770':
                    if "." not in utilities: item_loader.add_value("utilities", utilities)
                else:
                    utilities = desc.split("provision")[0].split("€")[-2].strip().split(" ")[-1]
                    item_loader.add_value("utilities", utilities)
                
            except: pass
        
        import dateparser
        if "libre " in desc.lower():
            available_date = desc.lower().split("libre")[1].split("loyer")[0].replace("-","").strip()
            if available_date:
                date_parsed = dateparser.parse(available_date.replace("308","30"), date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
                    
        if "dépôt de garantie" in desc.lower():
            deposit = desc.lower().split("dépôt de garantie")[1].strip().split(" ")[0]
            item_loader.add_value("deposit", int(float(deposit)))
        
        images=[x for x in response.xpath("//div[@class='slider_bien']/a/div/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        item_loader.add_value("landlord_name", "Agence immobilière Vitrolles")
        item_loader.add_value("landlord_phone", "04 42 41 86 70")
        item_loader.add_value("landlord_email", "vitrolles@nestenn.com")
        
        parking=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'garage')]/text()").get()
        if parking:
            item_loader.add_value("parking", True)
        
        terrace=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'terrasse')]/text()").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        energy_label=response.xpath("//div[contains(@class,'consoEner')]/@style//parent::div/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label)
            
        swimming_pool=response.xpath("//div[@id='groupeIcon']/div/div[contains(.,'piscine')]/text()").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool",True)

        yield item_loader.load_item()