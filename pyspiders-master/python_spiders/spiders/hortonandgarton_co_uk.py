# Author: Nipun Arora
# Team: Sabertooth
import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces, remove_unicode_char
import re
from scrapy import Selector
import lxml,js2xml

class HortonAndGartonCoUk(scrapy.Spider):
    name = "hortonandgarton_co_uk"
    allowed_domains = ["hortonandgarton.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale ='en'
    thousand_separator=','
    scale_separator='.'
    position = 0

    def start_requests(self):
        start_urls = [{
            'url':'https://www.hortonandgarton.co.uk/findhome/?department=residential-lettings&location=&property_type=90&price_range=&rent_range=&minimum_bedrooms=&radius=0&view=&pgp=',
            'property_type':'apartment'
        },
        {
            'url':'https://www.hortonandgarton.co.uk/findhome/?department=residential-lettings&location=&property_type=77&price_range=&rent_range=&minimum_bedrooms=&radius=0&view=&pgp=',
            'property_type':'house'
        }]

        for property_item in start_urls:
            yield scrapy.Request(url = property_item['url'],
                                callback = self.parse,
                                meta = {'request_url': property_item['url'],
                                'property_type':property_item['property_type']})
  
    def parse(self, response, **kwargs):

        total = response.xpath('//ul[@class="properties clear"]/li').extract()
        listings = response.xpath('//div[@class="status-to-let"]/../../@href').extract()
        for url in listings:
            yield scrapy.Request(
                url = response.urljoin(url),
                callback = self.get_property_details,
                meta = {'request_url' : response.urljoin(url),
                'property_type':response.meta.get('property_type')})
        
        if len(total)>=10:
            next_page_url = response.xpath('//a[@class="next page-numbers"]/@href').extract_first()
            if next_page_url:
                yield scrapy.Request(
                        url=next_page_url,
                        callback=self.parse,
                        meta={'request_url':next_page_url,
                        'property_type':response.meta.get('property_type')})

    def get_property_details(self, response):

        # external_link = response.meta.get('request_url')

        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Hortonandgarton_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('property_type',response.meta.get('property_type'))

        title = response.xpath('//head/title/text()').extract_first()
        if title:
            item_loader.add_value('title',title)
            item_loader.add_value('city',title.split(', ')[-1].replace(' - Horton and Garton',''))

        rent = extract_number_only(response.xpath('//h1[@class="pricetitle"]/text()').extract_first(),thousand_separator=',',scale_separator=',')
        if rent:
            item_loader.add_value('rent_string','£'+str(int(float(rent)*4)))

        address = response.xpath('//h1[@class="propertytitle"]/text()').extract_first()
        if address:
            item_loader.add_value('address',address)
            item_loader.add_value('zipcode',address.split(', ')[-1])

        room_count = extract_number_only(response.xpath('//li[@class="bedrooms"]/text()').extract_first())
        if room_count:
            item_loader.add_value('room_count',room_count)

        bathroom_count = extract_number_only(response.xpath('//li[@class="bathrooms"]/text()').extract_first())
        if bathroom_count:
            item_loader.add_value('bathroom_count',bathroom_count)

        item_loader.add_xpath('images','//ul[@class="slides"]//a/@href')
        item_loader.add_xpath('floor_plan_images','//div[@id="Floor"]//img/@src')

        features = ", ".join(response.xpath('//div[@class="features"]//li/text()').extract())
        description = " ".join(response.xpath('//div[@class="summary"]//p/text()').extract())
        if description:
            item_loader.add_value('description',description)

        epc = response.xpath('//li[contains(text(),"EPC")]/text()').extract_first()
        if epc:
            item_loader.add_value('energy_label',epc)

    
        #uncomment this for floor, but numeric values aren't present
        # floor = response.xpath('//li[contains(text(),"floor")]/text()').extract_first()
        # if floor:
        #     item_loader.add_value('floor',floor.replace('floor',''))

        javascript = response.xpath('//script[contains(text(),"LatLng")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            lat_long = xml_selector.xpath('.//var[@name="myLatlng"]//number/@value').extract()
            item_loader.add_value('latitude',lat_long[0])
            item_loader.add_value('longitude',lat_long[1])

        #https://www.hortonandgarton.co.uk/property/glenthorne-road-brackenbury-village-london/
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        if "swimming pool" in features.lower() or "swimming pool" in description.lower():
            item_loader.add_value('swimming_pool', True)
        
        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        if "furnished" in features.lower():
            if re.search(r"un[^\w]*furnished", features.lower()):
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        elif "furnished" in description.lower():
            if re.search(r"un[^\w]*furnished", description.lower()):
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)

        item_loader.add_xpath('landlord_name','//h3[@class="pinkName"]/text()')
        item_loader.add_xpath('landlord_phone','//div[@class="action-make-enquiry2"]/a/text()')
        item_loader.add_xpath('landlord_email','//span[@class="mailManager"]/text()')

        self.position+=1
        item_loader.add_value('position',self.position)

        yield item_loader.load_item()

    