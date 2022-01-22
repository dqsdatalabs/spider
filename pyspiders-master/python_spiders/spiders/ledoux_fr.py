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
import dateparser

class MySpider(Spider):
    name = 'ledoux_fr'
    execution_type='testing'
    country='france'
    locale='fr' # LEVEL 1

    def start_requests(self):
        start_urls = [
            {"url": "https://www.ledoux.fr/listing.html?loc=location&type=appartement&insee=&quartier=&piece=&surfacemin=&prixmin=&prixmax=&proximite2=&investisseur=&telhab=&neuf=&prestige=&p=1&tri=", 
            "property_type": "apartment"},
            {"url": "https://www.ledoux.fr/listing.html?loc=location&type=maison&insee=&quartier=&piece=&surfacemin=&prixmin=&prixmax=&proximite2=&investisseur=&telhab=&neuf=&prestige=&p=1&tri=",
             "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                            "type":url.get('type')})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get('page', 2)
        
        seen = False
        for item in response.xpath("//section[@class='col-md-9 col-md-push-3 Content']/article/figure/a/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 2 or seen:
            url = ''
            if response.meta.get('property_type') == 'apartment':
                url = f'https://www.ledoux.fr/listing.html?loc=location&type=appartement&insee=&quartier=&piece=&surfacemin=&prixmin=&prixmax=&proximite2=&investisseur=&telhab=&neuf=&prestige=&p={page}&tri='
            else:
                url = f'https://www.ledoux.fr/listing.html?loc=location&type=maison&insee=&quartier=&piece=&surfacemin=&prixmin=&prixmax=&proximite2=&investisseur=&telhab=&neuf=&prestige=&p={page}&tri='
            yield Request(url, callback=self.parse, meta={"page": page+1, 'property_type': response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))

        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Ledoux_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//h5[@class='lieu']/text()[2]").get()
        if address:
            address = address.strip()
            item_loader.add_value("address", address)

        square_meters = response.xpath("//span[contains(.,'Surface')]/text()").get()
        if square_meters:
            square_meters = str(int(float(square_meters.split(':')[1].split('m')[0].strip().replace(',', '.'))))
            item_loader.add_value("square_meters", square_meters)

        room_count = response.xpath("//span[contains(.,'Nombre de pièces')]/text()").get()
        if room_count:
            room_count = room_count.split(':')[1].strip().split(' ')[0]
            item_loader.add_value("room_count", room_count)

        rent = response.xpath("//div[@class='price']/text()").get()
        if rent:
            rent = rent.split('€')[0].strip().replace('\xa0', '').replace(' ', '')
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'EUR')

        external_id = response.url.split('-')[-1].split('.')[0]
        if external_id:
            external_id = external_id.strip()
            item_loader.add_value("external_id", external_id)

        description = response.xpath("//div[@class='description']/text()").getall()
        desc_html = ''      
        if description:
            for d in description:
                desc_html += d.strip() + ' '
            desc_html = desc_html.replace('\xa0', '')
            filt = HTMLFilter()
            filt.feed(desc_html)
            item_loader.add_value("description", filt.text)

        available_date = response.xpath("//span[contains(.,'Disponibilité')]/text()").get()
        if available_date:
            available_date = available_date.split(':')[1].strip().split(' ')[-1].strip()
            if len(available_date.split('-')) > 2 or len(available_date.split('.')) > 2 or len(available_date.split('/')) > 2:
                if available_date.isalpha() != True:
                    date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        images = [x for x in response.xpath("//div[@id='slider']/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", str(len(images)))
        
        deposit = response.xpath("//span[contains(.,'Dépôt de garantie')]/text()").get()
        if deposit:
            deposit = str(int(float(deposit.split(':')[1].split('€')[0].strip().replace(' ', ''))))
            item_loader.add_value("deposit", deposit)

        utilities = response.xpath("//span[contains(.,'Charges')]/text()").get()
        if utilities:
            utilities = str(int(float(utilities.split(':')[1].split('€')[0].strip().replace(' ', ''))))
            item_loader.add_value("utilities", utilities)

        energy_label = response.xpath("//img[contains(@src,'CE-')]/@src").get()
        if energy_label:
            energy_label = energy_label.split('-')[-1].split('.')[0].strip()
            energy_label_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
            if energy_label in energy_label_list:
                item_loader.add_value("energy_label", energy_label)

        parking = response.xpath("//span[contains(.,'Parking')]/text()").get()
        if parking:
            if int(float(parking.split(':')[1].strip())) > 0:
                item_loader.add_value("parking", True)

        balcony = response.xpath("//span[contains(.,'Balcon')]/text()").get()
        if balcony:
            if int(float(balcony.split(':')[1].strip())) > 0:
                item_loader.add_value("balcony", True)

        terrace = response.xpath("//span[contains(.,'Terrasse')]/text()").get()
        if terrace:
            if int(float(terrace.split(':')[1].strip())) > 0:
                item_loader.add_value("terrace", True)
        
        yield item_loader.load_item()

class HTMLFilter(HTMLParser):
    text = ''
    def handle_data(self, data):
        self.text += data