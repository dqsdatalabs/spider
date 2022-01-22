# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib,json
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces
import re


class TriorSpider(scrapy.Spider):
    name = 'trior_be'
    allowed_domains = ['www.trior.be']
    start_urls = ['http://www.trior.be/fr-BE/Products/Listing/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    api_url = 'http://www.trior.be/fr-BE/Products/Listing/'
    params = {"offset": 0,
              "limit": 12,
              "sort": "asc",
              "filter[purpose]": 2,
              "filter[category]": 1
              }
    position = 0
    thousand_separator = '.'
    scale_separator = ','

    def start_requests(self):
        start_urls = [
            {
                "filter[category]": 2,
                "property_type": "apartment"
             },
            {
                "filter[category]": 1,
                "property_type": "house"
            }
        ]
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["filter[category]"] = url["filter[category]"]
            yield scrapy.Request(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                                 callback=self.parse,
                                 meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                       'params': params1,
                                       'property_type': url["property_type"]})
        
    def parse(self, response, **kwargs):
        data = json.loads(response.body.decode("utf-8"))
        listings = data['listing']
        for listing in listings:
            yield scrapy.Request(
                url=listing["link"],
                callback=self.get_property_details,
                meta={'request_url': listing["link"],
                      'zip': listing["zip"],
                      'locality': listing["locality"],
                      'latitude': listing["latitude"],
                      'longitude': listing["longitude"],
                      'property_type': response.meta["property_type"]}
            )

        if len(listings) > 0:
            params1 = copy.deepcopy(response.meta["params"])
            if response.meta["params"]["offset"] == 0:
                params1["offset"] = 24
            else:
                params1["offset"] = response.meta["params"]["offset"] + 12
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=next_page_url,
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1,
                      'property_type': response.meta['property_type']}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        external_id = response.xpath('.//*[contains(text(), "Réf.")]/text()').extract_first()
        item_loader.add_value('external_id', external_id.split("Réf.")[1])
        item_loader.add_xpath('title', './/title/text()')
        # item_loader.add_value('property_type', response.meta["property_type"])

        item_loader.add_xpath('images', './/div[@class="item"]/img/@src')
        desc = "".join(response.xpath("//p[@class='desc']//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        else:
            desc = "".join(response.xpath("//p[@class='desc']//following-sibling::p//text()").getall())
            if desc:
                desc = re.sub('\s{2,}', ' ', desc.strip())
                item_loader.add_value("description", desc)
                
        item_loader.add_xpath('description', './/p[@class="desc"]/text()')
        item_loader.add_xpath('rent_string', './/span[@class="montant"]/text()')
        item_loader.add_xpath('city', './/div[contains(@class, "details")]/h1/text()')
        item_loader.add_value('zipcode', str(response.meta["zip"]))
        item_loader.add_value('latitude', response.meta["latitude"])
        item_loader.add_value('longitude', response.meta["longitude"])
        item_loader.add_value('address', ", ".join([response.meta["locality"], str(response.meta["zip"])]))

        utilities = "".join(response.xpath("//span[contains(.,'Charge')]//text()").getall())
        if utilities:
            if "-" in utilities:
                utilities = utilities.split(":")[1].split("-")[0].split("€")[0].strip()
            else:
                if "€" in utilities:
                    utilities = utilities.replace("provision","").split(" :")[-1].split("€")[0].strip()
                    
            if utilities.isdigit():
                item_loader.add_value("utilities", utilities)   

        # furnished
        # http://www.trior.be/fr-BE/Biens-immobiliers/3974/appartement/A-louer/molenbeek-saint-jean/appartement-biens-immobiliers-a-louer-molenbeek-saint-jean
        furnished = response.xpath('.//li[contains(text(),"Meublé")]/text()').extract_first()
        if furnished and 'Oui' in furnished:
            item_loader.add_value('furnished', True)
        elif furnished and 'Non' in furnished:
            item_loader.add_value('furnished', False)

        room_count = response.xpath('.//li[contains(text(),"Nombre de Chambres")]/text()').extract_first()
        if room_count:
            if extract_number_only(room_count) != "0":
                item_loader.add_value('room_count', extract_number_only(room_count))
            else:
                item_loader.add_value('room_count', '1')
                item_loader.add_value('property_type', 'studio')

        item_loader.add_value('property_type', response.meta["property_type"])
        bathroom_count = response.xpath('''.//li[contains(text(),"Salles d'eau")]/text()''').extract_first()
        if bathroom_count and '0' not in bathroom_count:
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))
        item_loader.add_xpath('square_meters', './/li[contains(text(),"habitable")]/text()')

        item_loader.add_value('landlord_name', 'TRIOR Rixensart')
        item_loader.add_value('landlord_phone', '02/652.50.50')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Trior_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
