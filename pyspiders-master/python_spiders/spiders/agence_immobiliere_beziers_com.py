# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only
import re 

class AgenceImmobiliereBeziersSpider(scrapy.Spider):
    name = "agence_immobiliere_beziers_com"
    allowed_domains = ["www.agence-immobiliere-beziers.com"]
    start_urls = [
        {
            'url':'https://www.agence-immobiliere-beziers.com/fr/annonces/louer-p-r70-5-1.html#menuSave=5&page=1&RgpdConsent=1609825387038&TypeModeListeForm=text&ope=2&multifiltre=8',
            'property_type':'house'
        },
        {
            'url':'https://www.agence-immobiliere-beziers.com/fr/annonces/louer-p-r70-5-1.html#menuSave=5&page=1&RgpdConsent=1609825387038&TypeModeListeForm=text&ope=2&multifiltre=2',
            'property_type':'apartment'
        }
    ]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    position = 0
    external_source="Agence_PySpider_france_fr"

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url.get('url'), 
                callback=self.parse,
                meta={'property_type':url.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@itemprop="itemListElement"]')
        for listing in listings:
            property_url = listing.xpath('.//a[@itemprop="url"]/@href').extract_first()
            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details, 
                meta={
                    'request_url':property_url,
                    'property_type':response.meta.get('property_type')})

    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        status = response.xpath("//h2[@class='detail-bien-type']/text()[contains(.,'ommercial')]").get()
        if status:
            return
        item_loader.add_value("external_link", response.meta.get('request_url'))

        external_id = response.xpath('.//span[@itemprop="productID"]/text()').extract()
        if external_id:
            item_loader.add_value("external_id", external_id[-1])

        p_type = response.xpath("//h2[@class='detail-bien-type']/text()").get()
        if p_type:
            if "appartement" in p_type.lower():
                item_loader.add_value("property_type", "apartment")
            elif "maison" in p_type.lower():
                item_loader.add_value("property_type", "house")
            elif "studio" in p_type.lower():
                item_loader.add_value("property_type", "studio")
            else:
                item_loader.add_value("property_type", response.meta.get('property_type'))
        else:
            item_loader.add_value("property_type", response.meta.get('property_type'))

        room_count = response.xpath('.//span[@class="ico-piece"]/following-sibling::text()').extract_first()
        if room_count:
            room_count = extract_number_only(room_count)
            if room_count==0:
                room_count='1'
                item_loader.add_value("property_type", 'studio')
            item_loader.add_value("room_count", room_count)

        address = response.xpath("//h2[@class='detail-bien-ville']/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
        
        utilities = response.xpath("//span[@class='charges_mens'][contains(.,'Provisions sur c')]/following-sibling::span/text()").get()
        if utilities:
            utilities = utilities.split(".")[0].replace(" ", "").strip()
            item_loader.add_value("utilities", utilities)

        square_meters = response.xpath('.//span[@class="ico-surface"]/following-sibling::text()').extract_first()
        if square_meters:
            square_meters = extract_number_only(square_meters)
            item_loader.add_value("square_meters", square_meters)

        city_zip = response.xpath('.//h2[@class="detail-bien-ville"]/text()').extract_first()
        if city_zip:
            city_zip = re.findall(r'(.+)\((\d+)\)',city_zip)
            item_loader.add_value("city", city_zip[0][0])
            item_loader.add_value("zipcode", city_zip[0][1])

        item_loader.add_xpath('title','.//h1/text()')

        deposit = response.xpath("//span[contains(text(),'Dépôt de garantie')]/following-sibling::span/text()").get()
        if deposit:
            item_loader.add_value("deposit", deposit.replace(" ",""))

        rent = response.xpath("//div[@class='detail-bien-prix hidden']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent.split("€")[0].strip().replace(" ",""))
        item_loader.add_value("currency", "EUR")
        item_loader.add_xpath('description','.//span[@itemprop="description"]//text()')

        item_loader.add_xpath('latitude','.//li[@class="gg-map-marker-lat"]/text()')
        item_loader.add_xpath('longitude','.//li[@class="gg-map-marker-lng"]/text()')

        item_loader.add_xpath('images','.//img[contains(@src,"assets")]/@src')

        item_loader.add_value('landlord_name','Agence Calvet')
        item_loader.add_value("landlord_email","contact@agencecalvet.com")
        item_loader.add_value('landlord_phone','04 67 28 43 73') 

        item_loader.add_value("external_source", self.external_source)
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
