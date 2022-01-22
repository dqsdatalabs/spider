import scrapy
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters
import re

class BradjlambrealtySpider(scrapy.Spider):
    name = 'bradjlambrealty'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    allowed_domains = ['bradjlambrealty.com']
    start_urls = ['https://www.bradjlambrealty.com/search?utf8=%E2%9C%93&a_comment_body=&s_q%5Blt%5D%5B%5D=rt&s_q%5Bmin_price%5D=0&s_q%5Bmax_price%5D=Unlimited&s_q%5Bquery%5D=&button=&s_q%5Bn_ids%5D=']

    def parse(self, response):
        for item in response.css("div.listings_wrapper .listing__wrapper .listing_link::attr(href)").getall():
            yield scrapy.Request(url=item, callback=self.parse_page)
        next_page = response.urljoin(response.css("nav.pagination span.next a::attr(href)").get())
        if next_page:
            yield scrapy.Request(url=next_page, callback=self.parse)


    def parse_page(self, response):

        title           = response.css("p.listing_main_address::text").get()
        address         = response.css("p.listing_main_address::text").get()
        city            = response.css("p.listing_main_address span::text").get()
        rent            = response.css(".main_price_float p.listing_main_price::text").get()
        room_count      = response.css("div.listing_amenities_container.beds_count p.single_amenity_count.bed_count::text").get()
        bathroom_count  = response.css("div.listing_amenities_container.baths_count p.single_amenity_count.bath_count::text").get()
        external_link   = response.url
        description     = response.css("p.listing_main_description::text").getall()[1]
        square_meters   = response.xpath('//p[contains(text(), "Lot Size: ")]/following-sibling::p/text()').get()
        parking         = response.xpath('//p[contains(text(), "Parking :")]/following-sibling::p/text()').get()
        images          = response.css(".slider div img::attr(data-lazy)").getall()
        external_id     = response.css(".mls::text").get()
        landlord_name   = response.css("div.show-agent-info h2::text").get()
        property_type   = response.xpath('//p[contains(text(), "Type: ")]/following-sibling::p/text()').getall()
        
        if square_meters:
            square_meters = sq_feet_to_meters(eval(square_meters.replace("X","*")))
        else:
            sq = response.css("p.listing_main_description::text").re("[0-9]+,*[0-9]*\W*[Ss][Qq]\.*\W*[Ff][Tt]")
            if sq:
                square_meters= sq_feet_to_meters(int(re.findall(r"[0-9]+,*[0-9]*",sq[-1])[0].replace(",","")))






        external_id     = external_id[external_id.index(":")+1:].strip()
        rent            = make_rent(rent)
        property_type   = make_property(property_type)





        item = ListingLoader(response=response)

        item.add_value("images"                 ,images)
        item.add_value("address"                ,address)
        item.add_value("rent"                   ,rent)
        item.add_value("square_meters"          ,int(int(square_meters)*10.764))
        item.add_value("room_count"             ,room_count)
        item.add_value("bathroom_count"         ,bathroom_count)
        item.add_value("title"                  ,title)
        item.add_value("description"            ,remove_white_spaces(description))
        item.add_value("currency"               ,"CAD")
        item.add_value("external_link"          ,external_link)
        item.add_value("landlord_name"          ,landlord_name)
        item.add_value("external_source"        ,self.external_source)
        item.add_value("property_type"          ,property_type)
        item.add_value("city"                   ,city)
        item.add_value("parking"                ,parking != "0")
        item.add_value("external_id"            ,external_id)

        if room_count != 0 and property_type.lower() not in  ["parking space",'rnt; locker']:
            yield item.load_item()



def make_rent(val):
    return int(float(val.replace("$","").replace(",","")))

def make_property(prop):
    for i in ["Storey", "Apartment", 'Loft','Bachelor/Studio','Condo','Sidesplit']:
        if i in  prop[1]:
            return "apartment"
    for i in ['House', 'Bungalow','Townhse','Townhouse']:
        if i in prop[1]:
            return 'house'
    if prop[1] == "Other" and prop[0] in ["Multiplex","Detached"]:
        return 'apartment'
  
    return prop[1]
