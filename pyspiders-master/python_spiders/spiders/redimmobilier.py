# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, extract_rent_currency


class RedimmobilierSpider(scrapy.Spider):
    name = 'redimmobilier_be'
    allowed_domains = ['redimmobilier.be']
    start_urls = ['https://www.redimmobilier.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.redimmobilier.be/fr/a-louer',
             'furnished_status': False
             },
            {'url': 'https://www.redimmobilier.be/fr/a-louer-meuble',
             'furnished_status': True
             }
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'request_url': url.get("url"),
                                       'furnished_status': url.get("furnished_status")})
            
    def parse(self, response, **kwargs):
        listings = response.xpath('.//*[@class="col-md-5"]//a/@href').extract()
        for property_url in listings:
            property_url = 'https://www.redimmobilier.be'+property_url[:-4]
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'furnished_status': response.meta.get('furnished_status')})
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        external_id = response.xpath('.//*[contains(text(),"Référence")]/text()').extract_first()
        item_loader.add_value('external_id', external_id.split(':')[1].strip())
        item_loader.add_xpath('title', ".//div[contains(@id,'property')]//h3[@class='leadingtext'][2]/text()")
        property_type = response.xpath('.//h4[(@class="subleadingtext")]//text()').extract_first()
        apartment_types = ["lejlighed", "appartement", "apartment", "piso", "flat", "atico",
                           "penthouse", "duplex", "dakappartement", "triplex"]
        house_types = ['hus', 'chalet', 'bungalow', 'maison', 'house', 'home', 'villa', 'huis']
        studio_types = ["studio", "flat/studio", "studio + terrasse"]
        
        if property_type.lower() in apartment_types:
            item_loader.add_value('property_type', "apartment")
        elif property_type.lower() in house_types:
            item_loader.add_value('property_type', "house")
        elif property_type.lower() in studio_types:
            item_loader.add_value('property_type', 'studio')
        else:
            return
        item_loader.add_xpath('description', './/*[contains(text(),"Description")]/following-sibling::div[1]/text()')
        item_loader.add_xpath('rent_string', './/h3[contains(text(), "€")]/text()')
        item_loader.add_xpath('bathroom_count', ".//td[contains(text(),'Salles de bain')]/following-sibling::td/text()")
        item_loader.add_xpath('square_meters', ".//td[contains(text(),'Habitable')]/following-sibling::td/text()")
        item_loader.add_value('furnished', response.meta.get('furnished_status'))
        city = response.xpath('.//h4[contains(@class, "leadingtext")]/preceding-sibling::h3[1]/text()').extract_first()
        item_loader.add_value('city', remove_white_spaces(city))
        
        item_loader.add_xpath('images', './/*[contains(@id,"sliderproperty")]//img/@data-src')
        
        room_count=response.xpath(".//td[contains(text(),'Chambre')]/following-sibling::td/text()").extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count)
        else:
            if property_type.lower() == "studio":
                item_loader.add_value('room_count', '1')
            elif "studio" in response.xpath('.//*[contains(@class,"col-md-0")]/h3[1]').extract_first().lower():
                # https://www.redimmobilier.be/fr/detail/10886
                # room_count
                item_loader.add_value('room_count', '1')

        address = response.xpath("//div[contains(@id,'property')]//h3[@class='leadingtext'][2]/text()").get()
        if address: item_loader.add_value("address", address.strip())

        utilities = response.xpath(".//td[contains(text(),'Charges')]/following-sibling::td/text()").extract_first()
        if utilities:
            item_loader.add_value('utilities', "".join(filter(str.isnumeric, utilities)))
        
        energy = response.xpath(".//td[contains(text(),'PEB')]/following-sibling::td/text()").extract_first()
        if energy:
            item_loader.add_value('energy_label', energy.split()[0])
        parking = response.xpath(".//td[contains(text(),'Garages')]/following-sibling::td/text()").extract_first()
        if parking:
            if parking.strip == "0":
                item_loader.add_value('parking', False)
            else:
                item_loader.add_value('parking', True)  
        terrace = response.xpath(".//td[contains(text(),'Terrasse')]/following-sibling::td/text()").extract_first()
        if terrace:
            if terrace.strip == "0":
                item_loader.add_value('terrace', False)
            else:
                item_loader.add_value('terrace', True)           
        item_loader.add_value('landlord_name', 'Red immobilier srl')
        item_loader.add_value('landlord_email', 'info@redimmobilier.be')
        item_loader.add_xpath('landlord_phone', './/*[contains(@class,"contact")]/../text()')

        if not item_loader.get_collected_values("landlord_phone"):
            item_loader.add_xpath("landlord_phone", "//span[@class='titlecontact']/following-sibling::text()")

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Redimmobilier_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
