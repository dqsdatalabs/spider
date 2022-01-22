# -*- coding: utf-8 -*-
# Author: Karan Katle
# Team: Sabertooth

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import extract_number_only, remove_white_spaces, convert_string_to_numeric
import re
import math


class KennethlloydsSpider(scrapy.Spider):
    name = "kennethlloyds_co_uk"
    allowed_domains = ["kennethlloyds.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    api_url = 'http://www.kennethlloyds.co.uk/results.asp'
    params = {
        'proptype': '',
        'displayperpage': 10,
        'BRANCH': 'KENNETHLLOYDS',
        'pricelow': 0,
        'propbedr': '',
        'Area_0': 'All_Areas',
        'AreaTotal': 18,
        'postcodes': '',
        'searchurl': '/searchrent.asp?',
        'pricehigh': 0,
        'displayorder': 'PriceAsk',
        'pricetype': 2,
        'market': 1,
        'search': 'Search',
        'offset': 0}
    listing_new = []
    position = 0

    def start_requests(self):
        start_urls = [
            {
                'proptype': 'Flat',
                "property_type": "apartment"
            },
            {
                'proptype': 'House',
                "property_type": "house"
            },
            {
                'proptype': 'Room To Let',
                "property_type": "room"
            }
        ]
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["proptype"] = url["proptype"]
            yield scrapy.Request(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                                 callback=self.parse,
                                 meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                       'params': params1,
                                       'property_type': url.get("property_type")})
                
    def parse(self, response, **kwargs):
        
        listings = response.xpath('.//table[contains(@class,"property")]')
        for listing in listings:
            property_url = listing.xpath('.//a[contains(@href,"detail")]/@href').extract_first()
            if property_url not in self.listing_new:
                self.listing_new.append(property_url)
                yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={
                        'request_url': response.urljoin(property_url),
                        "property_type": response.meta["property_type"]}
                )

        if len(response.xpath('.//a[contains(@href,"detail")]')) > 0 and self.position < len(self.listing_new):
            current_page = response.meta["params"]["offset"]
            params1 = copy.deepcopy(response.meta["params"])
            params1["offset"] = current_page + 10
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=self.api_url + "?" + urllib.parse.urlencode(params1),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1,
                      "property_type": response.meta["property_type"]}
            )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_xpath('description', './/*[@class="textjustify"]/text()')
        item_loader.add_xpath('external_id', './/*[contains(text(),"KENN_")]/text()')

        images = response.xpath('.//td[@align="center"]/img/@src').extract()
        if len(images) == 0:
            images = response.xpath('.//table[@align="center"]//img[contains(@src, "/pchomesdata/")]/@src').extract()
        if len(images) > 0:
            item_loader.add_value('images', images)

        rent_string = response.xpath('.//*[@class="detail_priceask"]/text()').extract_first()
        if 'pw' in rent_string:
            rent = convert_string_to_numeric(rent_string, KennethlloydsSpider)*4
            item_loader.add_value('rent_string', '£'+str(math.ceil(rent)))
        elif 'pcm' in rent_string or 'pm' in rent_string:
            item_loader.add_value('rent_string', remove_white_spaces(rent_string))
        address = response.xpath('.//*[@class="detail1a_addresssummary"]/text()').extract_first()
        item_loader.add_value('address', address)
        addr = address.split(',')
        if len(addr) == 3:
            item_loader.add_value('zipcode', remove_white_spaces(addr[-1]))
            item_loader.add_value('city', remove_white_spaces(addr[-2]))

        item_loader.add_value('title', address)
        features = ', '.join(response.xpath('.//*[@class="detailBullets"]//li//text()').extract()).lower()

        bathroom_count = re.search(r'\d+.(?=bathroom)', features)
        if bathroom_count and '0' not in bathroom_count.group():
            item_loader.add_value('bathroom_count', str(extract_number_only(bathroom_count.group())))

        room_count = re.search(r'\d+.(?=bedroom)', features)
        if room_count and '0' not in room_count.group():
            item_loader.add_value('room_count', str(extract_number_only(room_count.group())))
        elif item_loader.get_output_value('property_type') == 'room':
            item_loader.add_value('room_count', '1')
            
        if 'unfurnished' in features and ' furnished' not in features:
            item_loader.add_value('furnished', False)
        # http://www.kennethlloyds.co.uk/detail.asp?propcode=KENN_001166
        elif 'furnished' in features:
            item_loader.add_value('furnished', True)
        # http://www.kennethlloyds.co.uk/detail.asp?propcode=KENN_001166
        if 'washing machine' in features:
            item_loader.add_value('washing_machine', True)
        if 'balcony' in features:
            item_loader.add_value('balcony', True)
        if 'terrace' in features:
            item_loader.add_value('terrace', True)
        if 'dishwasher' in features:
            item_loader.add_value('dishwasher', True)
        if 'swimming pool' in features:
            item_loader.add_value('swimming_pool', True)
        if 'pets allowed' in features:
            item_loader.add_value('pets_allowed', True)
        # http://www.kennethlloyds.co.uk/detail.asp?propcode=KENN_000820
        if 'parking' in features:
            item_loader.add_value('parking', True)

        item_loader.add_value("external_source", "Kennethlloyds_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_xpath('landlord_name', './/*[contains(text(),"A:")]/following-sibling::text()[1]')
        item_loader.add_xpath('landlord_phone', './/*[contains(text(),"T:")]/following-sibling::text()[1]')
        item_loader.add_value('landlord_email', 'info@kennethlloyds.co.uk')
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
