# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import json
import copy
import urllib
from ..loaders import ListingLoader
from ..helper import format_date


class ModaLivingSpider(scrapy.Spider):
    name = 'modaliving_com'
    allowed_domains = ['modaliving.com']
    start_urls = ['https://api-mymoda.modaliving.com/lettings-search/search/sliding']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    api_url = "https://api-mymoda.modaliving.com/lettings-search/search/sliding"
    params = {"$top": 8,
              "$skip": 0,
              "$orderBy": "listPrice",
              "$filter": "term fn_InTerm 'fcd95992-560d-40ef-bc4f-be073adba388,dcd95992-560d-40ef-bc4f-be073adba389' and buildingId eq '1' and sharingCount fn_BySharingCount 1"}
    position = 0

    def start_requests(self):
        start_urls = [self.api_url + "?" + urllib.parse.urlencode(self.params)]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url,
                                       'params': self.params})

    def parse(self, response, **kwargs):
        listings = json.loads(response.body)['list']
        for listing in listings:
            unit_number = listing['unitNumber']
            property_url = "https://api-mymoda.modaliving.com/lettings-search/details-prs/{}".format(unit_number)
            yield response.follow(url=property_url,
                                  callback=self.get_property_details,
                                  meta={"request_url": property_url})
        if len(listings) > 0:
            skip = response.meta["params"]["$skip"]
            params1 = copy.deepcopy(self.params)
            params1["$skip"] = skip + 8
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        data = json.loads(response.body)['data']

        prop=""
        if "apartment" in data["desc"].lower():
            prop ="apartment"
        elif "house" in data["desc"].lower():
            prop = "house"
        elif "studio" in data["desc"].lower():
            prop = "studio"

        item_loader.add_value('property_type',prop)
        item_loader.add_value('external_link', response.meta['request_url'])
        item_loader.add_value('description', data["desc"])
        item_loader.add_value('external_id', data["unitNumber"])
        item_loader.add_value('title', data['address']['search'])

        if data['address']['city']:
            item_loader.add_value('city', data['address']['city'])
        if data['address']['postcode']:
            item_loader.add_value('zipcode', data['address']["postcode"])
        address = data['address']
        addressList = []
        for key in address:
            if key not in ['search', 'geo'] and address[key] and len(address[key]) != 0:
                addressList.append(address[key])
        addressString=", ".join(addressList)
        item_loader.add_value('address', addressString)

        item_loader.add_value('latitude', str(data['geo']['lat']))
        item_loader.add_value('longitude', str(data['geo']['lng']))
        
        item_loader.add_value('square_meters', str(data['totalSizeMsq']))
        item_loader.add_value('room_count', str(data['beds']))
        item_loader.add_value('bathroom_count', str(data['baths']))
        if data['availableFrom']:
            item_loader.add_value('available_date', format_date(data['availableFrom'].split("T")[0], "%Y-%m-%d"))

        item_loader.add_value('images', [image['url'] for image in data['images']])
        item_loader.add_value('floor_plan_images', [data['floorPlanUrl']])
        item_loader.add_value('rent', data['instalments'][0]['pricePer']['price'])
        item_loader.add_value('currency', data['instalments'][0]['pricePer']['currency'])
        item_loader.add_value('prepaid_rent', data['instalments'][0]['immediateFee']['price'])

        features = data['features']
        for feature in features:
            if "pet-friendly" in feature.values():
                item_loader.add_value('pets_allowed', feature['value'])
        if data['furnished'] == 'furnished':
            item_loader.add_value('furnished', True)
        if data['parking'] == "yes":
            item_loader.add_value('parking', True)
        else:
            item_loader.add_value('parking', False)
        item_loader.add_value('floor', data['level'])

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Modaliving_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
        


        
