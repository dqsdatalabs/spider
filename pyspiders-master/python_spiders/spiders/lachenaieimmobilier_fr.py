# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from urllib.parse import urljoin
from datetime import datetime
from datetime import date
import dateparser

class MySpider(Spider):
    name = 'lachenaieimmobilier_fr'
    execution_type='testing'
    country='france'
    locale='fr'
    external_source = "Lachenaieimmobilier_PySpider_france_fr"
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "http://www.lachenaieimmobilier.fr/category/location-1/maison-location/",
                "property_type" : "house"
            },
            {
                "url" : "http://www.lachenaieimmobilier.fr/category/location-1/appartement-location/",
                "property_type" : "apartment"
            },
            

        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@class,'thumbnail-hero')]/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
        
        next_page = response.xpath("//a[contains(@class,'next')]/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")}
            ) 
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        rented = response.xpath("//h1[@class='entry-title single']/text()[contains(.,'LOUE')]").extract_first()
        if rented:return

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", self.external_source)

        description = " ".join(response.xpath("//div[@class='content-text']//p/text()").getall()).strip()    
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        bathroom_count = response.xpath("//li[contains(.,\"Salle d'eau\") or contains(.,'Salle de bain')]/div[2]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        # date2 = ""
        # available_date = response.xpath("//p[contains(.,'Disponible')]/text()").get()
        # if available_date:
        #     if "compter" in available_date:
        #         date2 = available_date.split("du")[1].strip()
        #     elif "début" in available_date:
        #         date2 = available_date.split("début")[1].strip().replace("au plus tard","").strip()
        #     elif "fin" in available_date:
        #         date2 = available_date.split("fin")[1].strip()
        #     else:
        #         return
  
        #     date_parsed = dateparser.parse(
        #         date2, date_formats=["%m-%d-%Y"]
        #     )
        #     date3 = date_parsed.strftime("%Y-%m-%d")
        #     item_loader.add_value("available_date", date3)

            
        address = "".join(response.xpath("//h1[contains(@class,'entry-title single')]/text()").getall())
        if address:
            if "à" in address:
                address = address.strip().split("à")[1]
            elif "-" in address:
                address = address.strip().split("-")[-2]
            else:
                address = address.strip().split(" ")[-1]
            item_loader.add_value("address", address.strip())

        if title:
            if ' à ' in title or ' A ' in title:
                item_loader.add_value("city", title.split('|')[0].split(' à ')[-1].split(' A ')[-1].strip())
            else:
                city = "".join(response.xpath("//h1[@class='entry-title single']/text()").extract())
                if city:
                    item_loader.add_value("city", city.strip().split(" ")[-1])

        balcony = response.xpath("//li[contains(.,'Balcon')]/div[2]/strong/text()").get()
        if balcony:
            if int(balcony.strip()) > 0:
                item_loader.add_value("balcony", True)
            elif int(balcony.strip()) == 0:
                item_loader.add_value("balcony", False)
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)
            
        square_meters = response.xpath("//i[contains(following-sibling::text(),'Surface habitable')]/parent::div/following-sibling::div[1]/strong/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)
        elif description:
            if 'Il a une surface de' in description:
                item_loader.add_value("square_meters", description.split('Il a une surface de')[1].split(',')[0].split('m')[0].strip())

        room_count = response.xpath("//li//div[contains(.,'Pièce')]//following-sibling::div//strong//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = "".join(response.xpath("//h1[contains(@class,'entry-title single')]/text()").getall())
            if room_count:
                if "pièce" in room_count.lower():
                    room_count = room_count.strip().split(' ')[0]
                    item_loader.add_value("room_count", room_count)
                elif "studio" in room_count.lower():
                    item_loader.add_value("room_count", "1")
            elif title:
                if 'PIECE' in title:
                    item_loader.add_value("room_count", title.split('PIECE')[0].strip().split(' ')[-1].strip())

        # sale = response.xpath("//p[contains(.,'Prix')]/text()").get()
        # if sale:
        #     sale = sale.split(":")[1].split("€")[0].replace(" ","")
        #     print(sale)
        #     print(response.url)
        #     if len(sale)>5: return
        
        rent = response.xpath("//i[contains(following-sibling::text(),'Prix')]/parent::div/following-sibling::div[1]/strong/text()").get()
        if rent:
            rent = rent.split('€')[0].replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", 'EUR')

        

        external_id = response.xpath("//i[contains(following-sibling::text(),'Référence')]/parent::div/following-sibling::div[1]/strong/text()").get()
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        images = [x for x in response.xpath("//div[@class='row gallery-row']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//i[contains(following-sibling::text(),'Dépôt de garantie')]/parent::div/following-sibling::div[1]/strong/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace('\xa0', '').replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//i[contains(following-sibling::text(),'Performance énergétique')]/parent::div/following-sibling::div[1]/span/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//i[contains(following-sibling::text(),'Etage')]/parent::div/following-sibling::div[1]/strong/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        parking = response.xpath("//i[@class='icon-garage-count']/../following-sibling::div[1]/strong/text()").get()
        garage = response.xpath("//i[@class='icon-parking-count']/../following-sibling::div[1]/strong/text()").get()
        if parking: 
            if int(parking) > 0: item_loader.add_value("parking", True)
        elif garage:
            if int(garage) > 0: item_loader.add_value("parking", True)

        utilities = response.xpath("//i[@class='icon-plus']/../following-sibling::div[1]/strong/text()").get()
        if utilities:
            utilities = "".join(filter(str.isnumeric, utilities.split('.')[0]))
            if utilities.isnumeric(): item_loader.add_value("utilities", utilities)
        else:
            utilities = response.xpath("//div[@class='content-text']//p/text()[contains(.,'Charges')]").get()
            if utilities:
                utilities = utilities.split(":")[-1].strip().split(" ")[0]
                item_loader.add_value("utilities", utilities)

        elevator = response.xpath("//h2[.='Spécificité(s)']/following-sibling::div//i[contains(following-sibling::text(),'Ascenseur')]").get()
        if elevator:
            elevator = True
            item_loader.add_value("elevator", elevator)

        landlord_phone = response.xpath("//i[@class='icon-mobile']/following-sibling::text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split('/')[0].split(':')[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)
        item_loader.add_value("landlord_name", "La Chênaie Immobilier")
        item_loader.add_value("landlord_email", "transaction@lcimmo67.fr")

        status = response.xpath("//h1[contains(@class,'entry-title')][contains(.,'VENTE') or contains(.,'Vente') or contains(.,'vente')]").get()
        if not status:
            yield item_loader.load_item()