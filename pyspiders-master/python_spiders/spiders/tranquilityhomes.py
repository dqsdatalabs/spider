# -*- coding: utf-8 -*-
# Author: Valerii Nikitiuk
import scrapy
from scrapy import FormRequest
import re
from ..loaders import ListingLoader
import dateparser
from ..helper import remove_white_spaces, format_date

class TranquilityhomesSpider(scrapy.Spider):
    name = "tranquilityhomes"
    allowed_domains = ["www.tranquilityhomes.co.uk"]
    start_urls = (
        'http://www.www.tranquilityhomes.co.uk/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'http://www.tranquilityhomes.co.uk/property-search', 'property_type': 'apartment', 'token': 'lyG6Q9Qy1MPyezYd8VffMIHLTvRDOqHn1DGshV47', 'property_number':'2-13'},
            {'url': 'http://www.tranquilityhomes.co.uk/property-search', 'property_type': 'house', 'token': 'YvVUV6v7MjJmM6bgAkqKYCbvC2kz2AMKzRjUFpKe', 'property_number':'1'}
        ]
        for url in start_urls:
            data = {
                '_token': '{}'.format(url.get('token')),
                'department': 'Lettings',
                'minimum_price': '0',
                'maximum_price': '99999999',
                'minimum_rent': '0',
                'maximum_rent': '9999',
                'minimum_bedrooms': '0',
                'property_type': '{}'.format(url.get('property_number'))
            }
            yield FormRequest(
                url=url.get('url'),
                callback=self.parse, 
                formdata=data,
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[contains(@class, "property-container")]/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse property details
        property_type = response.meta.get('property_type')
        external_link = response.url
        external_id = response.xpath('//p[contains(text(), "Ref")]/following-sibling::p/text()').extract_first('').strip()
        room_count = response.xpath('//p[contains(text(), "Bedroom")]/following-sibling::p/text()').extract_first('').strip()
        bathrooms = response.xpath('//p[contains(text(), "Bathroom")]/following-sibling::p/text()').extract_first('').strip() 
        address = ''.join(response.xpath('//div[contains(@class, "has-text-centered")]//h1').extract()).strip()
        rent_string = ''.join(response.xpath('//div[contains(@class, "has-text-centered")]//h2/text()').extract()).strip()
        if 'pw' in rent_string.lower():
            rent_value = re.findall(r'([\d|,|\.]+)', rent_string)[0].replace(',', '')
            rent_month = str(int(rent_value) * 4) + '£'
        else:
            rent_month = rent_string 
        lat_lon_text = response.xpath('//iframe[contains(@src, "map")]/@src').extract_first()
        try:
            lat_lon = re.search(r'q=(.*?)&', lat_lon_text).group(1)
            latitude = lat_lon.split(',')[0]
            longitude = lat_lon.split(',')[-1] 
        except:
            latitude = ''
            longitude = ''
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        details = ''.join(response.xpath('//div[@id="description-tab-content-full"]//text()').extract()).lower()
        a_depos = details.split('\n')
        deposit = ''
        for a_depo in a_depos:
            if 'no deposit' not in a_depo.lower() and 'deposit' in a_depo.lower():
                if '.' in a_depo: 
                    for b_depo in a_depo.split('.'):
                        try:
                            deposit = re.findall(r'\d+', b_depo)[0]
                        except:
                            deposit = ''
                else:
                    try:
                        deposit = re.findall(r'\d+', a_depo)[0]
                    except:
                        deposit = ''
        furnished = ''
        date2 = ''
        for feature in response.xpath('//div[contains(@class, "is-one-third")]'):
            feature_text = feature.xpath('./ul/li/text()').extract_first()
            if 'furnished' in feature_text.lower():
                furnished = True
            elif 'available' in feature_text.lower():
                date_parsed = dateparser.parse(feature_text.lower().replace('available', ''), date_formats=["%d %B %Y"] ) 
                try:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                except:
                    date2 = ''
        title = response.xpath('//title/text()').extract_first('')
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('external_id', str(external_id))
        item_loader.add_value('title', title)
        item_loader.add_value('address', address)
        item_loader.add_xpath('description', '//div[@id="description-tab-content-summary"]/p/text()')
        item_loader.add_value('rent_string', rent_month)
        item_loader.add_xpath('images', '//div[@id="slider"]//img/@src')
        if latitude:
            item_loader.add_value('latitude', str(latitude))
        if longitude:
            item_loader.add_value('longitude', str(longitude))
        if furnished:
            item_loader.add_value('furnished', True)     
        if deposit:
            item_loader.add_value('deposit', deposit)
        if date2:
            item_loader.add_value("available_date", date2)
        item_loader.add_value('room_count', str(room_count))
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('bathroom_count', str(bathrooms))
        item_loader.add_value('landlord_name', 'Tranquilityhomes')
        item_loader.add_value('landlord_email', 'leicester@tranquilityhomes.co.uk')
        item_loader.add_value('landlord_phone', '0116 235 5232')
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()