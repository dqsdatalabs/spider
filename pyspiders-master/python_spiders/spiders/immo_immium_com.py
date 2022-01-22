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
from html.parser import HTMLParser
from datetime import datetime
from datetime import date
import dateparser
import re

class MySpider(Spider):
    name = 'immo_immium_com'
    execution_type='testing'
    country='france'
    locale='fr'
    
    def start_requests(self):

        start_urls = [
            {
                "url" : "http://www.immo.immium.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=2&tri=d_dt_crea&cp=,&",
                "property_type" : "house"
            },
            {
                "url" : "http://www.immo.immium.com/recherche,basic.htm?idqfix=1&idtt=1&idtypebien=1&tri=d_dt_crea&cp=,&",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)

        seen = False
        for item in response.xpath("//a[@itemprop='url']/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            seen = True
        
        if page == 2 or seen:
            if response.meta.get("property_type") == "apartment":
                p_url = f"http://www.immo.immium.com/recherche,incl_recherche_prestige_ajax.htm?cp=%2C&surfacemin=Min&surfacemax=Max&surf_terrainmin=Min&surf_terrainmax=Max&px_loyermin=Min&px_loyermax=Max&idqfix=1&idtt=1&pres=prestige&idtypebien=1&lang=fr&annlistepg={page}&tri=d_dt_crea&_=1603966790723"
                yield Request(
                url=p_url,
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type"), "page":page+1}
            ) 
              
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value("property_type", response.meta.get('property_type')) 
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Immoimmium_PySpider_"+ self.country + "_" + self.locale)

        bathroom_count = response.xpath("//div[contains(text(),\"Salle d'eau\") or contains(text(),'Salle de bain')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        utilities = response.xpath("//li[contains(text(),'Charges')]/text()").get()
        if utilities:
            item_loader.add_value("utilities", utilities.split(':')[-1].split('€')[0].strip())

        title =response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub("\s{2,}", " ", title.strip().split(":")[0]))

        available_date = response.xpath("//p[contains(text(),'Disponible le')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split(':')[-1].strip(), date_formats=["%d/%m/%Y"], languages=['fr'])
            today = datetime.combine(date.today(), datetime.min.time())
            if date_parsed:
                result = today > date_parsed
                if result == True:
                    date_parsed = date_parsed.replace(year = today.year + 1)
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        address = response.xpath("//h1/br/following-sibling::text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", address.split('(')[-1].split(')')[0].strip())

        latitude_longitude = response.xpath("//script[contains(.,'LATITUDE')]/text()").get()
        if latitude_longitude:
            item_loader.add_value("longitude", latitude_longitude.split(',LONGITUDE: "')[2].split('"')[0].strip().replace(',', '.'))
            item_loader.add_value("latitude", latitude_longitude.split(',LATITUDE: "')[2].split('"')[0].strip().replace(',', '.'))

        square_meters = response.xpath("//div[.='Surface']/following-sibling::div/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//ul/li/div[contains(.,'Pièce')]/following-sibling::div/text()").get()
        room=response.xpath("//ul/li/div[contains(.,'Chambre')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        elif room:
            item_loader.add_value("room_count", room.strip())

        rent = response.xpath("//span[@itemprop='price']/text()").get()
        if rent:
            rent = rent.strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//span[contains(.,'Référence')]/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@itemprop='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        city = response.xpath("//h1[@itemprop='name']/text()[2]").get()
        if city:
            city = city.split('(')[0].strip()
            item_loader.add_value("city", city)

        images = [x for x in response.xpath("//div[@id='slider']/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//strong[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            deposit = deposit.split(':')[1].split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            if deposit != 'N/A':
                item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//p[.='Consommations énergétiques']/parent::div//div[contains(@class,'dpe-bloc-lettre')]/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            if energy_label != 'VI':
                item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//div[.='Etage']/following-sibling::div/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        elevator = response.xpath("//div[.='Ascenseur']/following-sibling::div/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)
        
        balcony = response.xpath("//div[.='Balcon']/following-sibling::div/text()").get()
        if balcony:
            if int(balcony.strip()) > 0:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//div[.='Terrasse']/following-sibling::div/text()").get()
        if terrace:
            terrace = True
            item_loader.add_value("terrace", terrace)

        landlord_name = response.xpath("//div[@id='detail-agence-nom']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//div[@class='margin-top-10']//span[@id='numero-telephonez-nous-detail']/text()[2]").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data