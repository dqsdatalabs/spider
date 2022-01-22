# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
from  geopy.geocoders import Nominatim
from html.parser import HTMLParser
from urllib.parse import urljoin

class MySpider(Spider):
    name = 'nation_kwimmo_fr'
    execution_type='testing'
    country='france'
    locale='fr'

    def start_requests(self):

        start_urls = [
            {
                "url" : "https://www.nation.kwimmo.fr/catalog/advanced_search_result.php?action=update_search&search_id=&C_28=Location&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_28_search=EGAL&C_28_type=UNIQUE&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=1&cfamille_id_tmp=1&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_loc=0",
                "property_type" : "apartment"
            },
            {
                "url" : "https://www.nation.kwimmo.fr/catalog/advanced_search_result.php?action=update_search&search_id=1681696031127979&C_28=Location&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_28_search=EGAL&C_28_type=UNIQUE&cfamille_id_search=CONTIENT&cfamille_id_type=TEXT&cfamille_id=2&cfamille_id_tmp=2&C_34_search=COMPRIS&C_34_type=NUMBER&C_34_MIN=&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MIN=&C_30_MAX=&C_30_loc=0",
                "property_type" : "house"
            },
        ] #LEVEL-1

        for url in start_urls:
            yield Request(url=url.get('url'),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[@id='listing_bien']/a/@href").getall():
            f_url = response.urljoin(item)
            yield Request(
                f_url, 
                callback=self.populate_item, 
                meta={"property_type" : response.meta.get("property_type")},
            )

        next_page = response.xpath("//span[.='Suivante']/../@href").get()
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

        item_loader.add_value("external_source", "Nationkwimmo_PySpider_"+ self.country + "_" + self.locale)

        department = response.xpath("//p[.='DÉPARTEMENT']/parent::div/following-sibling::div/p/text()").get()
        commune = response.xpath("//p[.='COMMUNE']/parent::div/following-sibling::div/p/text()").get()
        region = response.xpath("//p[.='RÉGION']/parent::div/following-sibling::div/p/text()").get()
        address = ''
        if department:
            address += department.strip() + ' '
        if commune:
            address += commune.strip() + ' '
        if region:
            address += region.strip() + ' '
        if address:
            item_loader.add_value("address", address)

        square_meters = response.xpath("//strong[.='Surface']/following-sibling::p/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split('m')[0].strip())))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//strong[.='Chambres']/following-sibling::p/text()").get()
        if room_count:
            room_count = room_count.strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//span[@class='hono_inclus_price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace(' ', '').replace('\xa0', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.xpath("//div[@class='ref']/text()").get()
        if external_id:
            external_id = external_id.strip().strip('REF').strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//p[@class='description col-xs-12']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        zipcode = response.xpath("//p[.='CODE POSTAL']/parent::div/following-sibling::div/p/text()").get()
        if zipcode:
            zipcode = zipcode.strip()
            item_loader.add_value("zipcode", zipcode)

        images = [urljoin('https://www.nation.kwimmo.fr', x) for x in response.xpath("//div[@id='flex_slider_bien']//a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))

        parking = response.xpath("//strong[.='Nombre places parking']/following-sibling::p/text()").get()
        if parking:
            if int(parking.strip()) > 0:
                parking = True
                item_loader.add_value("parking", parking)

        elevator = response.xpath("//strong[.='Ascenseur']/following-sibling::p/text()").get()
        if elevator:
            if elevator.strip().lower() == 'oui':
                elevator = True
            elif elevator.strip().lower() == 'non':
                elevator = False
            if type(elevator) == bool:
                item_loader.add_value("elevator", elevator)

        landlord_name = response.xpath("//p[@class='identite']/text()").get()
        if landlord_name:
            landlord_name = landlord_name.strip()
            item_loader.add_value("landlord_name", landlord_name)

        landlord_phone = response.xpath("//p[@class=' num-tel']/text()").get()
        if landlord_phone:
            landlord_phone = landlord_phone.strip()
            item_loader.add_value("landlord_phone", landlord_phone)

        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data