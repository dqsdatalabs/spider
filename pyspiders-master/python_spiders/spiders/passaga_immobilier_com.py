# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only
import js2xml
import lxml
from scrapy import Selector
from datetime import datetime 
import dateparser
import re
class PassagaImmobilierComSpider(scrapy.Spider):
    name = "passaga_immobilier_com"
    allowed_domains = ["www.passaga-immobilier.com"]
    start_urls = [
        {
            'url':'https://www.passaga-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1687941870795732&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=2&C_27_tmp=2&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=',
            'property_type':'house',
        },
        # {
        #     'url':'https://www.passaga-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1687941870795732&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=',
        #     'property_type':'apartment',
        # },
        {
            'url':'https://www.passaga-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1687941870795732&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=MaisonMaitre&C_27_tmp=MaisonMaitre&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX=',
            'property_type':'house',
        },
    ]
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url=url.get('url'), 
                callback=self.parse,
                meta={'property_type':url.get('property_type')})
    p_info = True
    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="product-listing"]')
        for listing in listings:
            property_url = response.urljoin(listing.xpath('./a/@href').extract_first())
            # print(property_url)
            yield scrapy.Request(
                url=property_url, 
                callback=self.get_property_details, 
                meta={'request_url':property_url,
                    'property_type':response.meta.get('property_type')})
        
        next_page_url = response.xpath('.//li[@class="next-link active"]/a/@href').extract_first()
        if next_page_url:
            next_page_url = response.urljoin(next_page_url)
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse,
                                 meta={'property_type': response.meta.get('property_type')
                                       })
        elif self.p_info:
            self.p_info = False
            url = "https://www.passaga-immobilier.com/catalog/advanced_search_result.php?action=update_search&search_id=1687941870795732&map_polygone=&C_28_search=EGAL&C_28_type=UNIQUE&C_28=Location&C_27_search=EGAL&C_27_type=TEXT&C_27=1&C_27_tmp=1&C_34_MIN=&C_34_search=COMPRIS&C_34_type=NUMBER&C_30_search=COMPRIS&C_30_type=NUMBER&C_30_MAX=&C_65_search=CONTIENT&C_65_type=TEXT&C_65=&C_64_search=INFERIEUR&C_64_type=TEXT&C_64=&keywords=&C_33_MAX=&C_30_MIN=&C_38_MIN=&C_38_search=COMPRIS&C_38_type=NUMBER&C_38_MAX="
            yield scrapy.Request(url= url,
                                callback=self.parse,
                                meta={
                                "property_type":"apartment",
                                })
    def get_property_details(self, response, **kwargs):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('request_url'))
        item_loader.add_value("property_type", response.meta.get('property_type'))

        title = response.xpath(".//h1/text()").get()
        if title:
            title = re.sub('\s{2,}', ' ', title.strip())
            item_loader.add_value("title", title)
        available_date = response.xpath('.//div[text()="Disponibilité"]/following-sibling::div/b/text()').extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"], languages=['fr'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                if date_parsed.strftime("%Y") < str(datetime.now().year):
                    return 
                else:
                    item_loader.add_value("available_date", date2)
        room_count = response.xpath('.//div[contains(text(),"chambre(s)")]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count',extract_number_only(room_count,thousand_separator=',',scale_separator='.'))
        elif not room_count:
            room1=response.xpath('.//div[contains(text(),"pièce(s)")]/text()').extract_first()
            if room1:
                room=re.findall("\d+",room1) 
                item_loader.add_value('room_count',room)


        bathroom_count = response.xpath('.//div[@class="value"][contains(text(),"salle(s) d\'eau") or contains(text(),"de bain")]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count',extract_number_only(bathroom_count,thousand_separator=',',scale_separator='.'))

        external_id = response.xpath('.//div[@class="product-ref"]/text()').extract_first()
        if external_id:
            item_loader.add_value('external_id',external_id.split(' : ')[1])

        city_zipcode = response.xpath('.//div[@class="product-localisation"]/text()').extract_first()
        if city_zipcode:
            item_loader.add_value('city',city_zipcode.split()[1])
            item_loader.add_value('zipcode',city_zipcode.split()[0])
            item_loader.add_value('address',city_zipcode.split()[1]+', '+city_zipcode.split()[0])

        desc = " ".join(response.xpath(".//div[@class='product-description']/text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        deposit = response.xpath('.//div[contains(text(),"Dépôt de Garantie")]/following-sibling::div/b/text()').extract_first()
        if deposit:
            item_loader.add_value('deposit',deposit.split(".")[0])

        utilities = response.xpath('.//div[contains(text(),"Provision sur charges")]/following-sibling::div/b/text()').extract_first()
        if utilities:
            item_loader.add_value('utilities',utilities.split(".")[0])
        rent = response.xpath('//div[contains(text(),"Loyer mensuel HC")]/following-sibling::div/b/text()').extract_first()
        if rent:
            item_loader.add_value('rent',rent.split(".")[0].split("EUR")[0])
            item_loader.add_value('currency', "EUR")
            
        energy_label = response.xpath('.//div[contains(text(),"Valeur conso annuelle énergie")]/following-sibling::div/b/text()').extract_first()
        if energy_label:
            item_loader.add_value('energy_label',energy_label)

        square_meters = response.xpath('.//div[text()="Surface"]/following-sibling::div/b/text()').extract_first()
        if square_meters:
            item_loader.add_value('square_meters',square_meters.split(".")[0].split("m")[0])

        floor = response.xpath('.//div[text()="Etage"]/following-sibling::div/b/text()').extract_first()
        if floor:
            item_loader.add_value('floor',floor)

        parking = response.xpath('.//div[text()="Nombre places parking" or text()="Nombre garages/Box"]/following-sibling::div/b/text()').extract_first()
        if parking:
            if parking=='0':
                item_loader.add_value('parking',False)
            else:
                item_loader.add_value('parking',True)

        balcony = response.xpath('.//div[text()="Nombre balcons"]/following-sibling::div/b/text()').extract_first()
        if balcony:
            if balcony=='0':
                item_loader.add_value('balcony',False)
            else:
                item_loader.add_value('balcony',True)

        terrace = response.xpath('.//div[text()="Nombre de terrasses"]/following-sibling::div/b/text()').extract_first()
        if terrace:
            if terrace=='0':
                item_loader.add_value('terrace',False)
            else:
                item_loader.add_value('terrace',True)

        images = response.xpath('.//div[@class="item-slider"]/img[contains(@src,"office8")]/@src').extract()
        images = [response.urljoin(image) for image in images]
        item_loader.add_value('images',images)

        furnished = response.xpath("//span[@class='alur_location_meuble']/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        javascript = response.xpath('.//script[contains(text(),"lat") and contains(text(),"lng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)

            latitude = selector.xpath('.//var[@name="myLatlng"]/new/arguments/number[1]/@value').extract_first()
            longitude = selector.xpath('.//var[@name="myLatlng"]/new/arguments/number[2]/@value').extract_first()

            if latitude and longitude:
                item_loader.add_value('latitude', latitude)
                item_loader.add_value('longitude', longitude)

        item_loader.add_xpath('landlord_name','.//div[@class="name-agence"]/text()')
        item_loader.add_xpath('landlord_phone','.//div[@class="tel-agence"]/a/text()')

        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.split('_')[0].capitalize(), self.country, self.locale))
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
