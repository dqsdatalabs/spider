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

class MySpider(Spider):
    name = 'kappaimmobilier_com'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.kappaimmobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1682499703256505&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_30_MIN=&C_36_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&keywords=",
                "property_type" : "house"
            },
            {
                "url" : "https://www.kappaimmobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_33_search=COMPRIS&C_33_type=NUMBER&C_33_MIN=&C_33_MAX=&C_34_MAX=&C_30_MIN=&C_36_MAX=&C_36_MIN=&C_36_search=COMPRIS&C_36_type=NUMBER&keywords=",
                "property_type" : "apartment"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@class='photo-product']/a/@href").extract():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )
            

        next_page = response.xpath("//li[contains(@class,'next-link')]/a/@href").get()
        if next_page:
            yield Request(
                url=response.urljoin(next_page),
                callback=self.parse,
                meta={"property_type" : response.meta.get("property_type")},
            )
           
        
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Kappaimmobilier_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_xpath("title","//div/h1/text()")
        
        latitude_longitude = response.xpath("//script[contains(.,'LatLng')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('LatLng(')[1].split(',')[0].strip()
            longitude = latitude_longitude.split('LatLng(')[1].split(',')[1].split(')')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)

        city = response.xpath("//div[@class='container']/div/div/h1/text()").extract_first()
        if city:
            item_loader.add_value("city", city)

        address_zip = response.xpath("//div[@class='container']/div/div/div[@class='product-localisation']/text()").extract_first()
        if address_zip:
            item_loader.add_value("zipcode", address_zip.split(" ")[0].strip())
            item_loader.add_value("address", "{} {}".format(city,address_zip))

        square_meters = response.xpath("//div[contains(text(),'Surface')]/following-sibling::div/b/text()").get()
        if square_meters:
            square_meters = square_meters.split(':')[-1].split('m')[0].strip()
            square_meters = square_meters.replace('\xa0', '').replace(',', '.').replace(' ', '.').strip()
            square_meters = str(int(float(square_meters)))
            item_loader.add_value("square_meters", square_meters)

        room_count1 = response.xpath("//div[contains(text(),'Nombre pièces')]/following-sibling::div/b/text()").get()
        room_count2 = response.xpath("//div[contains(text(),'Chambres')]/following-sibling::div/b/text()").get()
        room_count = ''
        if room_count2:
            room_count = room_count2
        else:
            room_count = room_count1
        if room_count:
            room_count = room_count.split(':')[-1].strip().replace('\xa0', '').split(' ')[0].strip()
            room_count = str(int(float(room_count)))
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = response.xpath("//div[contains(text(),'Salle')]/following-sibling::div/b/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        rent = response.xpath("//div[contains(text(),'Loyer charges')]/following-sibling::div/b/text()").get()
        if rent:
            rent = rent.split('EUR')[0].strip().replace('\xa0', '').replace(',', '.').replace(' ', '')
            rent = str(int(float(rent)))
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        utilities = response.xpath("//span[contains(@class,'alur_location_charges')]//text()").get()
        if utilities:
            utilities = utilities.split(":")[1].split("€")[0].strip()
            if "." in utilities:
                utilities = utilities.split(".")[0]
            item_loader.add_value("utilities", utilities)

        external_id = response.xpath("//span[contains(.,'Ref')]/text()").get()
        if external_id:
            external_id = external_id.split(':')[1].strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='desc-text']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        images = [urljoin('https://www.kappaimmobilier.com' ,x) for x in response.xpath("//div[@id='slider_product']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//div[contains(text(),'Dépôt de Garantie')]/following-sibling::div/b/text()").get()
        if deposit:
            deposit = str(int(float(deposit.split('EUR')[0].strip().replace(' ', '').replace(',', '.'))))
            item_loader.add_value("deposit", deposit)

        energy_label = response.xpath("//div[contains(text(),'Conso Energ')]/following-sibling::div/b/text()").get()
        if energy_label:
            energy_label = energy_label.strip()
            item_loader.add_value("energy_label", energy_label)

        floor = response.xpath("//div[.='Etage']/following-sibling::div/b/text()").get()
        if floor:
            floor = floor.strip().split(' ')[0]
            item_loader.add_value("floor", floor)

        parking = response.xpath("//div[contains(text(),'parking') or contains(text(),'garage')]/following-sibling::div/b/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        furnished = response.xpath("//div[contains(text(),'Meublé')]/following-sibling::div/b/text()[contains(.,'Oui')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(text(),'Ascenseur')]/following-sibling::div/b/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        landlord_name = response.xpath("//div[@class='item-agence']/h3[1]/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//li[contains(.,'Tél.')]/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.split(':')[1].strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data