# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import re, json
from ..loaders import ListingLoader
from python_spiders.helper import format_date 
import dateparser

def extract_city_zipcode(_address):
    city = _address.split(", ")[-2].strip()
    zipcode = _address.split(", ")[-1].strip()
    return city, zipcode

class KallarsSpider(scrapy.Spider):
    name = "kallars"
    allowed_domains = ["kallars.com"]
    start_urls = (
        'http://www.kallars.com/',
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'

    def start_requests(self):
        start_urls = [
            {'url': 'https://kallars.com/?id=37418&action=view&route=search&view=list&input=SE8&jengo_property_for=2&jengo_category=1&jengo_radius=50&jengo_property_type=13&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_bathrooms=0&jengo_max_bathrooms=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=on&pfor_offer=on&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper', 'property_type': 'apartment'},
            {'url': 'https://kallars.com/?id=37418&action=view&route=search&view=list&input=SE8&jengo_property_for=2&jengo_category=1&jengo_radius=50&jengo_property_type=6&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_bathrooms=0&jengo_max_bathrooms=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=on&pfor_offer=on&trueSearch=&searchType=postcode&latitude=&longitude=#total-results-wrapper', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//div[@class=" cardInfoProperites"]/h4/a')
        for link in links: 
            url = response.urljoin(link.xpath('./@href').extract_first())
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type')})
        if response.xpath('//a[contains(text(), "Next")]/@href'):
            next_link = response.urljoin(response.xpath('//a[contains(text(), "Next")]/@href').extract_first())
            yield scrapy.Request(url=next_link, callback=self.parse, dont_filter=True, meta={'property_type': response.meta.get('property_type')})

    def get_property_details(self, response):
        # parse details of the property
        property_type = response.meta.get('property_type')
        external_link = response.url
        title = response.xpath('//meta[@property="og:title"]/@content').extract_first()
        if not title:
            title = "".join(response.xpath("//head/title//text()").getall())
        address = response.xpath('//div[@class="property_detail__infos"]//h4/text()').extract_first('').strip()
        city, zipcode = extract_city_zipcode(address) 
        try:
            room_count = int(response.xpath('//span[contains(text(), "Bedroom")]/preceding-sibling::span/text()').extract_first(''))
        except:
            room_count =  ''
        lat = re.search(r'\"latitude\":\"(.*?)\"', response.text).group(1)
        lon = re.search(r'\"longitude\":\"(.*?)\"', response.text).group(1)
        rent_string = response.xpath('//div[@class="Letdetails detail___price"]/text()').extract_first('').strip()
        if room_count: 
            item_loader = ListingLoader(response=response)
            item_loader.add_value('property_type', property_type)
            item_loader.add_value('external_link', external_link)
            item_loader.add_value('title', title)
            item_loader.add_value('address', address)

            bath = "".join(response.xpath("//span[contains(.,'Bathroom')]/span/text()").getall())
            if bath:
                bath = bath.split("Bathroom")[0].strip()
                item_loader.add_value("bathroom_count", bath)
            
            features = "".join(response.xpath("//ul[@class='details__features']/li/text()").getall())
            if features:
                if "furnished or unfurnished" in features.lower():
                    item_loader.add_value("furnished", True)
                elif "unfurnished" in features.lower():
                    item_loader.add_value("furnished", False)
                elif "furnished" in features.lower():
                    item_loader.add_value("furnished", True)

                
                if "sqft" in features.lower():
                    sqm = features.split("sqft")[0].strip().split(" ")[-1].split(".")[0].strip()
                    if sqm and sqm.isnumeric():
                        item_loader.add_value("square_meters", sqm)
                elif "sq ft" in features.lower():
                    sqm = features.split("sq ft")[0].strip().split(" ")[-1].split(".")[0].strip()
                    if sqm and sqm.isnumeric():
                        item_loader.add_value("square_meters", sqm)
            

            item_loader.add_value('city', address)
            item_loader.add_value('zipcode', zipcode)
            available_date = response.xpath('//span[contains(@class, "pd__availability")]/text()').extract_first('')
            date_parsed = dateparser.parse( available_date, date_formats=["%d %B %Y"] ) 
            try:
                date2 = date_parsed.strftime("%Y-%m-%d")
            except:
                try:
                    date2 =  format_date(available_date, '%d %B %Y')
                except:
                    date2 = ''
            if date2:
                item_loader.add_value("available_date", date2)  
            item_loader.add_xpath('description', '//div[@id="description"]//p//text()')
            item_loader.add_value('rent_string', rent_string)
            item_loader.add_xpath('images', '//div[@id="details-photo"]//@src')
            item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('latitude', str(lat))
            item_loader.add_value('longitude', str(lon))
            item_loader.add_value('landlord_name', 'Kallars London')
            item_loader.add_value('landlord_email', 'admin@kallars.com')
            item_loader.add_value('landlord_phone', '020 3848 1399')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item() 