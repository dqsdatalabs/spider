# -*- coding: utf-8 -*-
# Author: Pankaj Kalania
# Team: Sabertooth

import scrapy
import json
import urllib
from ..loaders import ListingLoader
import copy


class ImmoTrustSpider(scrapy.Spider):
    name = 'immo_trust_com'
    allowed_domains = ['immo-trust.com', 'realty.itcl.io']
    start_urls = ['https://www.immo-trust.com/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0
    api_url = "https://realty.itcl.io/estates"
    headers = {
        "authorization": "G8oh7iGO_1WC3K8TBQgKzAp3f3BNI7C3iQTQydR41f4",
        "id": "immotrust"
    }
    params = {
        'limit': 15,
        'currentPage': 1,
        'purpose': 'FOR_RENT',
        'purposeList': ['FOR_RENT', 'RENTED'],
        'deleted': 0,
    }

    def start_requests(self):
        yield scrapy.Request(url=self.api_url,
                             callback=self.parse,
                             method="POST",
                             meta={'request_url': self.api_url,
                                   "payload": self.params},
                             headers=self.headers,
                             body=json.dumps(self.params)
                             )

    def parse(self, response, **kwargs):
        listings = json.loads(response.body)['Result']
        if listings:
            for listing in listings:
                if listing["category"] in ["FLAT", "HOUSE", "APARTMENT"]:
                    request_url = "https://www.immo-trust.com/fr-be/a-louer/" + urllib.parse.quote(listing["category"].lower()) + "/" + urllib.parse.quote(listing["city"]) + "/" + listing["id"]
                    yield scrapy.Request(url=request_url,
                                        callback=self.get_property_details,
                                        meta={'request_url': request_url,
                                            'data_json': listing}
                                        )

        if len(listings) == 15:
            params1 = copy.deepcopy(response.meta["payload"])
            current_page = params1["currentPage"]
            params1["currentPage"] = current_page + 1
            yield scrapy.Request(url=self.api_url,
                                 callback=self.parse,
                                 meta={'request_url': self.api_url,
                                       "payload": params1},
                                 headers=self.headers,
                                 body=json.dumps(params1)
                                 )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta["request_url"])
        rented = response.xpath("//span[@class='ribbon']//text()[.='Loué']").extract_first()
        if rented:
            return

        data_json = response.meta["data_json"]
        item_loader.add_value('external_id', data_json["id"].split("_")[-1])  

        title = " ".join(response.xpath("//title/text()").extract()) 
        item_loader.add_value('title', title.strip())      
        item_loader.add_value('description', data_json['description']['fr'])
        item_loader.add_value('city', data_json['city'])
        item_loader.add_value('zipcode', data_json['zip'])
        item_loader.add_value('address', ", ".join([data_json["address"], data_json["city"], data_json["zip"]]))
        if data_json['location']:
            item_loader.add_value('latitude', str(data_json['location']['coordinates'][1]))
            item_loader.add_value('longitude', str(data_json['location']['coordinates'][0]))
        property_mapping = {"flat": "apartment"}
        property_type = data_json['category'].lower()
        for key_i in property_mapping:
            property_type = property_type.replace(key_i, property_mapping[key_i])
        item_loader.add_value('property_type', property_type)
        if "area" in data_json:
            item_loader.add_value('square_meters', str(data_json['area']))
        if "roomCount" in data_json:
            item_loader.add_value('room_count', str(data_json['roomCount']))
        elif "studio" in data_json['description']['fr']:
            item_loader.add_value('room_count', 1)
        if "bathCount" in data_json:
            item_loader.add_value('bathroom_count', str(data_json['bathCount']))
        if data_json['pictures']:
            item_loader.add_value('images', [image['url'] for image in data_json['pictures']])
        item_loader.add_value('rent_string', " ".join([str(data_json['price']), data_json['currency']]))
        if data_json['epc'] != "":
            item_loader.add_value('energy_label', data_json["epc"])

        # utilities
        # https://www.immo-trust.com/fr-be/a-louer/flat/Etterbeek/whoman_4041605
        item_loader.add_xpath('utilities', './/span[contains(text(), "charges:")]/../span[@class="value"]/b/text()')

        item_loader.add_value('furnished', data_json["furnished"])
        item_loader.add_value('floor', str(data_json["floor"]))

        # parking
        if data_json["parkingCount"]:
            if data_json["parkingCount"] == 0:
                item_loader.add_value('parking', False)
            else:
                item_loader.add_value('parking', True)

        # terrace
        if data_json["terraceCount"]:
            if data_json["terraceCount"] == 0:
                item_loader.add_value('terrace', False)
            else:
                item_loader.add_value('terrace', True)

        if "lave-vaisselle" in data_json['description']['fr']:
            item_loader.add_value('dishwasher', True)
        if "machine à laver" in data_json['description']['fr']:
            item_loader.add_value('washing_machine', True)
        if "piscine" in data_json['description']['fr']:
            item_loader.add_value('swimming_pool', True)

        item_loader.add_value('landlord_name', "ImmoTrust")
        item_loader.add_value('landlord_email', "info@immo-trust.com")
        item_loader.add_value('landlord_phone', "+3227709990")

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "ImmoTrust_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
