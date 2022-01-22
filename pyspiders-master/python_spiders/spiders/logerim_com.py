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
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser

class MySpider(Spider):
    name = 'logerim_com'
    execution_type='testing'
    country='france'
    locale='fr'
    download_timeout = 180

    def start_requests(self):

        start_urls = [
            {
                "url" : "http://www.logerim.com/resultats?transac=location&type%5B%5D=maison&budget_maxi=&surface_mini=",
                "property_type" : "house"
            },
            {
                "url" : "http://www.logerim.com/resultats?transac=location&type%5B%5D=appartement&budget_mini=&budget_maxi=&surface_mini=",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        script_data = response.xpath("//script[contains(.,'properties')]/text()").get()
        try:
            data = script_data.split("properties = ")[1]
            data_size = len(data.split("lien\": "))
            for i in range(1,data_size):
                f_url = "http://www.logerim.com/" + data.split("lien\": ")[i].split(",")[0].strip("\"")
                yield Request(
                    f_url, 
                    callback=self.populate_item, 
                    meta={
                        "property_type" : response.meta.get("property_type"),    
                    },
                )
        except:
            return      
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Logerim_PySpider_"+ self.country + "_" + self.locale)
        
        title = "".join(response.xpath("//div//h1[@class='titre']//text()").getall())
        if title:
            item_loader.add_value("title",title.strip())

        address = response.xpath("//li/address/text()").get()
        if address:
            item_loader.add_value("address", address)

        square_meters = response.xpath("//li[contains(.,'Surface Habitable :')]/strong/text()").get()
        if square_meters:
            square_meters = square_meters.split(':')[-1].split('m')[0].strip()
            square_meters = square_meters.replace('\xa0', '').replace(',', '.').replace(' ', '.').strip()
            square_meters = str(int(float(square_meters)))
            item_loader.add_value("square_meters", square_meters)

        bathroom_count = response.xpath("//div[@id='collapseDetails']//li[contains(.,'Salle de bain')]/strong/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        room_count = response.xpath("//li[contains(.,'Nombre de chambre')]/strong/text()").get()
        if room_count:
            room_count = room_count.split(':')[-1].strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
        elif not room_count:
            room_count1=response.xpath("//li[contains(.,'Nombre de pièce(s)')]/strong/text()").get()
            if room_count1:
                room_count1 = room_count1.split(':')[-1].strip().replace('\xa0', '').split(' ')[0].strip()
                room_count1 = str(int(float(room_count1)))
                item_loader.add_value("room_count", room_count1)


        rent = response.xpath("//span[@class='text-primary-color']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '')
            rent = rent.replace(',', '').replace('.', '')
            rent = rent.split(' ')
            reg_rent = [] 
            for i in rent:
                if i.isnumeric():
                    reg_rent.append(i)
            r = "".join(reg_rent)
            item_loader.add_value("rent", r)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//span[contains(.,'Ref.')]/text()").get()
        if external_id:
            external_id = external_id.split('Ref.')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//h1[@class='titre']/parent::div/p/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [urljoin('http://www.logerim.com' ,x) for x in response.xpath("//div[@id='carousel']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//li[contains(.,'Dépôt de garantie')]/strong/text()").get()
        if deposit:
            deposit = deposit.split('€')[0].strip().replace(' ', '').replace(',', '').replace('.', '')
            item_loader.add_value("deposit", deposit)

        parking = response.xpath("//li[contains(.,'Nombre de Parking')]/strong/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        elevator = response.xpath("//li[contains(.,'Ascenseur')]/strong/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        balcony = response.xpath("//li[contains(.,'Nombre de Balcon')]/strong/text()").get()
        if balcony:
            if int(balcony.strip()) > 0:
                balcony = True
                item_loader.add_value("balcony", balcony)

        terrace = response.xpath("//li[contains(.,'Nombre de terrasse')]/strong/text()").get()
        if terrace:
            if int(terrace.strip()) > 0:
                terrace = True
                item_loader.add_value("terrace", terrace)

        landlord_name = response.xpath("//strong[@itemprop='name']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//a[@itemprop='telephone']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        landlord_email = response.xpath("//span[@itemprop='email']/text()").get()
        if landlord_email:
            landlord_email = landlord_email.strip()
            item_loader.add_value("landlord_email", landlord_email)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data