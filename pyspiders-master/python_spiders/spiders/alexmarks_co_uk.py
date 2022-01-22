# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import js2xml
import lxml
import scrapy
from scrapy import Selector
import re
from ..helper import extract_number_only
from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class AlexmarksSpider(scrapy.Spider):
    name = 'alexmarks_co_uk'
    allowed_domains = ['alexmarks.co.uk']
    start_urls = ['https://www.alexmarks.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        property_types = [
            {"parameter": "House",
             "property_type": "house"
             },
            {"parameter": "Apartment",
             "property_type": "apartment"
             },
            {"parameter": "Flat",
             "property_type": "apartment"
             }
        ]
        for property_param in property_types:
            prop_types = property_param.get('parameter')
            url = f'https://www.alexmarks.co.uk/results.asp?view=grid&proptype={prop_types}&statustype=1&searchurl=%2Fresults%2Easp%3F&market=1&pricetype=3'
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'property_type': property_param.get('property_type')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="resultsgrid"]')
        for property_item in listings:
            url = property_item.xpath('.//a/@href').extract_first()
            address = property_item.xpath('.//div[@class="detail-contain"]/h2/text()').extract_first()
            yield scrapy.Request(
                url= response.urljoin(url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(url),
                      'property_type': response.meta.get('property_type'),
                      'address': address}
            )

        next_page_url = response.xpath('.//a[contains(@title,"next page")]/@href').extract_first()
        next_page_enable = response.xpath('.//a[contains(@title,"next page")]/parent::li/@class').extract_first()
        if next_page_url and next_page_enable != "disabled":
            yield scrapy.Request(
                url=response.urljoin(next_page_url),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'property_type': response.meta.get('property_type')}
            )

    def get_property_details(self, response):
        external_link = response.meta.get('request_url')
        item_loader = ListingLoader(response=response)
        item_loader.add_value('address', response.meta.get('address'))
        location = None
        javascript = response.xpath('.//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[0]
            longitude = xml_selector.xpath('.//identifier[@name="LatLng"]/../../../arguments/number/@value').extract()[1]

            item_loader.add_value('latitude', str(round(float(latitude), 3)))
            item_loader.add_value('longitude', str(round(float(longitude), 3)))

            if location and 'address' in location.raw:
                if 'postcode' in location.raw['address']:
                    item_loader.add_value('zipcode', location.raw['address']['postcode'])
                if 'city' in location.raw['address']:
                    item_loader.add_value('city', location.raw['address']['city'])
        if not location:
            item_loader.add_value('city', item_loader.get_output_value('address').split(',')[-2])
            item_loader.add_value('zipcode', item_loader.get_output_value('address').split(',')[-1])

        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', external_link)
        # not sure if externalid should be only the numbers or the full term. left it as full term.
        item_loader.add_value('external_id', external_link.split('/')[-2])
        item_loader.add_xpath('title', './/div[contains(@class,"detail-header")]//a/text()')
        
        # sometimes may be per week and ssometimes per month
        # item_loader.add_xpath('rent_string', './/span[@class="priceask"]/text()')
        rent_string = response.xpath('.//span[@class="priceask"]/text()').extract_first().replace(",","")
        if rent_string:
            if "pw" in rent_string:
                rent = float(re.findall(r"\d[\d.,]*", rent_string)[0])
                rent_string = re.sub(r"\d[\d.,]*", str(rent * 4), rent_string)
                item_loader.add_value('rent_string', rent_string)
            else:
                item_loader.add_value('rent_string', rent_string)
        item_loader.add_xpath('description', './/div[contains(@class,"detail-content")]/p/text()')
        
        item_loader.add_xpath('images', './/img[@class="sp-image"]/@src')
        energy_label = response.xpath('.//td[@class="epcCurrent"]//img[contains(@src, "/energy/")]/@src').extract_first()
        if energy_label:
            item_loader.add_value('energy_label', re.findall(r"(?<=/energy/)\d+", energy_label)[0])
        item_loader.add_value('room_count', extract_number_only(item_loader.get_output_value('title')))
        if response.xpath('.//div[@class="bullets-li"]/p[contains(text(),"bathroom")]/text()').extract_first():
            item_loader.add_value('bathroom_count', extract_number_only(response.xpath('.//div[@class="bullets-li"]/p[contains(text(),"bathroom")]/text()').extract_first()))
        item_loader.add_value('landlord_name', 'alexmarks')
        item_loader.add_value('landlord_phone', '020 7078 7950')
        item_loader.add_value('landlord_email', 'info@alexmarks.co.uk')

        features = ' '.join(response.xpath('.//div[@class="bullets-li"]/p/text()').extract())

        # ex https://www.alexmarks.co.uk/property/seven-sisters-road-finsbury-park-n4/alexm-002971/1 as street off parking
        if "parking" in features.lower() or 'parking' in item_loader.get_output_value('description').lower():
            item_loader.add_value('parking', True)
        # ex https://www.alexmarks.co.uk/property/seven-sisters-road-finsbury-park-n4/alexm-002971/1 present in description
        if "balcony" in features.lower() or "balcony" in item_loader.get_output_value('description').lower():
            item_loader.add_value('balcony', True)
        # ex https://www.alexmarks.co.uk/property/wray-crescent-finsbury-park-n4/alexm-002952/1 prsent in description
        if "terrace" in features.lower() or "terrace" in item_loader.get_output_value('description').lower():
            item_loader.add_value('terrace', True)
        #ex https://www.alexmarks.co.uk/property/pooles-park-finsbury-park-n4/alexm-002833/1 present in description
        if "furnished" in features.lower() or "furnished" in item_loader.get_output_value('description').lower():
            item_loader.add_value('furnished', True)

        # https://www.alexmarks.co.uk/property/chapel-market-islington-n1/alexm-003024/1
        if "washing machine" in features or "washing machine" in item_loader.get_output_value('description').lower():
            item_loader.add_value('washing_machine', True)

        # epec value not present as text..it is in image like https://www.alexmarks.co.uk/Shared/images/carbon/graph.gif
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Alexmarks_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()
